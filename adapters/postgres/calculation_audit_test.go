package postgres_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	audits "github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"testing"
	"time"
)

func recordCalculationFixture(t *testing.T, ctx context.Context, engine *facts.Engine, store *evidence.Postgres, c model.Caller, snapshot model.FactSnapshot) model.CalculationRecord {
	t.Helper()
	artifact := func(id, content string) model.ArtifactReference {
		ref, err := store.Put(ctx, c.Tenant, id, []byte(content))
		if err != nil {
			t.Fatal(err)
		}
		_, err = engine.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: model.Command{Key: id, Purpose: "calculation"}, Evidence: model.Evidence{ID: id, Scope: "case1", SourceSystem: "external-application", SourceObject: id, SourceRevision: "v1", Speaker: "application", SourceClass: "bank_record", SourceTime: time.Now().UTC(), MediaType: "application/json", Classification: "confidential", RetentionPolicy: "calculation-artifacts", Object: ref}})
		if err != nil {
			t.Fatal(err)
		}
		return model.ArtifactReference{EvidenceID: id, Digest: ref.Digest}
	}
	input := artifact("calculation-input", `{"normalized":"A"}`)
	output := artifact("calculation-output", `{"result":"external"}`)
	request := model.CalculationReceipt{Command: model.Command{Key: "calculation-record", Purpose: "calculation"}, Scope: "case1", SnapshotID: snapshot.ID, ApplicationID: "application1", ExternalRunID: "run1", MethodID: "external-method", MethodVersion: "v1", ExecutableDigest: model.BytesDigest([]byte("executable")), ConfigurationDigest: model.BytesDigest([]byte("config")), NormalizationVersion: "v1", Input: input, Output: output, StartedAt: time.Now().Add(-time.Minute), CompletedAt: time.Now(), Outcome: "succeeded"}
	result, err := engine.RecordCalculation(ctx, c, request)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := engine.RecordCalculation(ctx, c, request)
	if err != nil || retry.ID != result.ID || retry.LedgerRevision != result.LedgerRevision {
		t.Fatal(retry, err)
	}
	differentCommand := request
	differentCommand.Key = "same-external-run"
	retry, err = engine.RecordCalculation(ctx, c, differentCommand)
	if err != nil || retry.ID != result.ID {
		t.Fatal(retry, err)
	}
	changed := differentCommand
	changed.Key = "changed-external-run"
	changed.ConfigurationDigest = model.BytesDigest([]byte("different-config"))
	if _, err := engine.RecordCalculation(ctx, c, changed); !errors.Is(err, model.ErrConflict) {
		t.Fatal("changed run accepted", err)
	}
	changed = request
	changed.Key = "bad-artifact"
	changed.ExternalRunID = "bad-run"
	changed.Input.Digest = model.BytesDigest([]byte("not-the-artifact"))
	if _, err := engine.RecordCalculation(ctx, c, changed); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("mismatched artifact", err)
	}
	t.Run("remote calculation", func(t *testing.T) { testRemoteCalculation(t, ctx, engine, c, request) })
	// Independent command keys racing for the same external run converge to one receipt.
	var wg sync.WaitGroup
	type outcome struct {
		record model.CalculationRecord
		err    error
	}
	results := make(chan outcome, 2)
	for _, key := range []string{"concurrent-calc-a", "concurrent-calc-b"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			r := request
			r.Key = key
			r.ExternalRunID = "concurrent-run"
			record, err := engine.RecordCalculation(ctx, c, r)
			results <- outcome{record, err}
		}(key)
	}
	wg.Wait()
	close(results)
	var prior string
	for out := range results {
		if out.err != nil {
			t.Fatal(out.err)
		}
		if prior != "" && prior != out.record.ID {
			t.Fatal("duplicate calculation")
		}
		prior = out.record.ID
	}
	return result
}
func testAuditExports(t *testing.T, ctx context.Context, ledger *postgres.Ledger, store *evidence.Postgres, c model.Caller, admin *pgxpool.Pool) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := audits.NewEd25519("test-signing-key", private)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := facts.NewWithAudit(ledger, allow{}, store, model.AuditOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	crossScope, err := engine.CreateFactSnapshot(ctx, c, model.FactSnapshotRequest{
		Command:   model.Command{Key: "cross-scope-audit-snapshot", Purpose: "audit"},
		Selectors: []model.FactKey{{Scope: "case1", Subject: "absent", Predicate: "absent"}, {Scope: "case2", Subject: "absent", Predicate: "absent"}},
		ValidAt:   time.Now().UTC().Truncate(time.Microsecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := model.AuditExportRequest{Command: model.Command{Key: "full-audit", Purpose: "audit"}, Scope: "case1", IncludeEvidence: true}
	pending, err := engine.StartAuditExport(ctx, c, request)
	if err != nil || pending.State != "pending" {
		t.Fatal(pending, err)
	}
	query := model.AuditExportQuery{ID: pending.ID, Scope: "case1", Purpose: "audit"}
	completed, err := engine.RunAuditExport(ctx, c, query)
	if err != nil || completed.State != "completed" || completed.Bundle == nil {
		t.Fatal(completed, err)
	}
	keys := map[string]ed25519.PublicKey{"test-signing-key": public}
	if err := audits.Verify(*completed.Bundle, keys); err != nil {
		t.Fatal(err)
	}
	if completed.Bundle.Manifest.ThroughRevision >= completed.CompletedRevision || completed.Artifact.Version != completed.Artifact.Digest {
		t.Fatal("incorrect export boundary", completed)
	}
	var scopeGap bool
	for _, gap := range completed.Bundle.Manifest.Gaps {
		if gap.Kind == "snapshot" && gap.ID == crossScope.ID && gap.Reason == "outside_export_scope" {
			scopeGap = true
		}
	}
	if !scopeGap {
		t.Fatal("missing cross-scope exclusion gap")
	}
	var oldSnapshot, calculation, currentCorrection bool
	for _, entry := range completed.Bundle.Entries {
		if entry.Kind == "snapshot" && entry.ID == crossScope.ID {
			t.Fatal("cross-scope snapshot disclosed")
		}
		switch entry.Kind {
		case "snapshot":
			var snap model.FactSnapshot
			if err := json.Unmarshal(entry.Payload, &snap); err != nil {
				t.Fatal(err)
			}
			if len(snap.Items) > 0 && snap.Items[0].Resolution.Value != nil && snap.Items[0].Resolution.Value.Text != nil && *snap.Items[0].Resolution.Value.Text == "A" {
				oldSnapshot = true
			}
		case "calculation":
			var r model.CalculationRecord
			if err := json.Unmarshal(entry.Payload, &r); err != nil {
				t.Fatal(err)
			}
			if r.Receipt.ExternalRunID == "run1" {
				calculation = true
			}
		case "event":
			var event model.Event
			if err := json.Unmarshal(entry.Payload, &event); err != nil {
				t.Fatal(err)
			}
			if event.Type == "supersede_fact" {
				currentCorrection = true
			}
		}
	}
	if !oldSnapshot || !calculation || !currentCorrection {
		t.Fatal("incomplete lineage", oldSnapshot, calculation, currentCorrection)
	}
	tampered := *completed.Bundle
	tampered.Entries = append([]model.AuditEntry(nil), tampered.Entries...)
	tampered.Entries[0].Payload = json.RawMessage(`{}`)
	if audits.Verify(tampered, keys) == nil {
		t.Fatal("tamper accepted")
	}
	tampered = *completed.Bundle
	tampered.Entries = tampered.Entries[:len(tampered.Entries)-1]
	if audits.Verify(tampered, keys) == nil {
		t.Fatal("missing entry accepted")
	}
	tampered = *completed.Bundle
	tampered.Manifest.Scope = "another-scope"
	if audits.Verify(tampered, keys) == nil {
		t.Fatal("scope mutation accepted")
	}
	if err := audits.Verify(*completed.Bundle, map[string]ed25519.PublicKey{}); err == nil {
		t.Fatal("untrusted signing key accepted")
	}
	replay, err := engine.RunAuditExport(ctx, c, query)
	if err != nil || replay.Artifact.Digest != completed.Artifact.Digest || replay.CompletedRevision != completed.CompletedRevision {
		t.Fatal(replay, err)
	}
	again, err := engine.StartAuditExport(ctx, c, request)
	if err != nil || again.ID != completed.ID {
		t.Fatal(again, err)
	}
	noEvidence, err := facts.NewWithAudit(ledger, denyPermission{permission: "can_read_evidence"}, store, model.AuditOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("remote audit", func(t *testing.T) { testRemoteAudit(t, ctx, engine, noEvidence, c, public) })
	if _, err := noEvidence.GetAuditExport(ctx, c, query); !errors.Is(err, model.ErrDenied) {
		t.Fatal("full export disclosed without evidence permission", err)
	}
	for _, entry := range completed.Bundle.Entries {
		if entry.Kind == "calculation" {
			if _, err := noEvidence.GetCalculationReceipt(ctx, c, model.CalculationQuery{ID: entry.ID, Purpose: "audit"}); !errors.Is(err, model.ErrDenied) {
				t.Fatal("calculation artifacts disclosed after evidence access revoked", err)
			}
			other := c
			other.Tenant = "other-tenant"
			if _, err := engine.GetCalculationReceipt(ctx, other, model.CalculationQuery{ID: entry.ID, Purpose: "audit"}); !errors.Is(err, model.ErrDenied) {
				t.Fatal("cross-tenant calculation disclosed", err)
			}
		}
	}
	request.Key = "metadata-audit"
	request.IncludeEvidence = false
	metadata, err := noEvidence.StartAuditExport(ctx, c, request)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err = noEvidence.RunAuditExport(ctx, c, model.AuditExportQuery{ID: metadata.ID, Scope: "case1", Purpose: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range metadata.Bundle.Entries {
		if entry.Kind == "evidence_content" {
			t.Fatal("evidence bytes disclosed")
		}
		if entry.Kind == "version" {
			var v model.Version
			json.Unmarshal(entry.Payload, &v)
			for _, span := range v.Proposal.Sources {
				if span.Quote != "" {
					t.Fatal("quote disclosed")
				}
			}
		}
	}
	if err := audits.Verify(*metadata.Bundle, keys); err != nil {
		t.Fatal(err)
	}
	request.Key = "fencing-audit"
	job, err := engine.StartAuditExport(ctx, c, request)
	if err != nil {
		t.Fatal(err)
	}
	var old, newLease model.AuditExportJob
	err = ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		var err error
		old, err = tx.(model.AuditTx).ClaimAuditJob(ctx, job.ID, time.Minute)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	err = ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		_, err := tx.(model.AuditTx).ClaimAuditJob(ctx, job.ID, time.Minute)
		return err
	})
	if !errors.Is(err, model.ErrNotFound) {
		t.Fatal("live lease stolen", err)
	}
	if _, err := admin.Exec(ctx, `UPDATE audit_export_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND id=$2`, c.Tenant, job.ID); err != nil {
		t.Fatal(err)
	}
	err = ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		var err error
		newLease, err = tx.(model.AuditTx).ClaimAuditJob(ctx, job.ID, time.Minute)
		return err
	})
	if err != nil || newLease.Fence <= old.Fence {
		t.Fatal(newLease, err)
	}
	err = ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		bundle := *completed.Bundle
		bundle.Manifest.ExportID = job.ID
		if err := tx.(model.AuditTx).StageAuditBundle(ctx, old, bundle); err != nil {
			return err
		}
		_, err := tx.Append(ctx, model.Event{ID: "stale-export-attempt", Type: "complete_audit_export", Scope: "case1", EntityID: job.ID, Caller: c})
		return err
	})
	if !errors.Is(err, model.ErrRevision) {
		t.Fatal("stale completion accepted", err)
	}
	var n int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM ledger_events WHERE tenant_id=$1 AND id='stale-export-attempt'`, c.Tenant).Scan(&n); err != nil || n != 0 {
		t.Fatal("fenced attempt leaked ledger event", n, err)
	}
	if _, err := admin.Exec(ctx, `UPDATE audit_export_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND id=$2`, c.Tenant, job.ID); err != nil {
		t.Fatal(err)
	}
	recovered, err := engine.RunAuditExport(ctx, c, model.AuditExportQuery{ID: job.ID, Scope: "case1", Purpose: "audit"})
	if err != nil || recovered.State != "completed" {
		t.Fatal(recovered, err)
	}
}

type denyPermission struct{ permission string }

func (d denyPermission) Check(ctx context.Context, r model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	decision, err := (allow{}).Check(ctx, r)
	if r.Permission == d.permission {
		decision.Allowed = false
	}
	return decision, err
}
