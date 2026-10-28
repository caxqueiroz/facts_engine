package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"testing"
	"time"
)

type unavailableIndex struct{ model.RetrievalIndex }

func (unavailableIndex) Candidates(context.Context, string, string, string, string, int) ([]model.SearchCandidate, error) {
	return nil, model.ErrUnavailable
}

type failedContextLedger struct{ model.Ledger }
type failedContextTx struct{ model.ContextTx }

func (t failedContextTx) Append(context.Context, model.Event) (model.Event, error) {
	return model.Event{}, model.ErrUnavailable
}
func (l failedContextLedger) Transact(ctx context.Context, tenant string, fn func(model.Tx) error) error {
	return l.Ledger.Transact(ctx, tenant, func(tx model.Tx) error { return fn(failedContextTx{tx.(model.ContextTx)}) })
}

func testContexts(t *testing.T, ctx context.Context, e *facts.Engine, l *postgres.Ledger, store *evidence.Postgres, c model.Caller, index model.RetrievalIndex, q model.RecallRequest, admin *pgxpool.Pool) {
	t.Helper()
	r := model.ContextRequest{Command: model.Command{Key: "context-one", Purpose: q.Purpose}, Scope: "case1", Query: q}
	testRetrievalInterruption(t, ctx, l, store, l, index, c, r)
	m, err := remotePrepareContext(t, ctx, e, c, r)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Items) == 0 || len(m.Rendered) == 0 || m.RenderedDigest != model.BytesDigest(m.Rendered) || m.Digest != model.ContextDigest(m) || m.CreationRevision <= m.SelectionRevision {
		t.Fatal("invalid retained context", m)
	}
	for _, want := range []error{context.Canceled, context.DeadlineExceeded, model.ErrUnavailable} {
		interrupted, err := facts.NewWithRetrieval(l, allow{}, interruptedEvidenceStore{store, want}, model.RetrievalOptions{Index: index, Progress: l, Generation: "g1"})
		if err != nil {
			t.Fatal(err)
		}
		recalled, err := interrupted.Recall(ctx, c, q)
		if !errors.Is(err, want) || len(recalled.Items) != 0 {
			t.Fatal("interrupted recall returned partial results", recalled, err)
		}
		for _, mixed := range []bool{false, true} {
			request := r
			request.Key = "interrupted-context"
			request.IncludeMemories = mixed
			manifest, err := interrupted.PrepareContext(ctx, c, request)
			if !errors.Is(err, want) || manifest.ID != "" || len(manifest.Rendered) != 0 {
				t.Fatal("interrupted context delivered", manifest, err)
			}
		}
	}
	broken, err := facts.NewWithRetrieval(l, allow{}, store, model.RetrievalOptions{Index: unavailableIndex{index}, Progress: l, Generation: "g1"})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := remotePrepareContext(t, ctx, broken, c, r)
	if err != nil || retry.ID != m.ID || !bytes.Equal(retry.Rendered, m.Rendered) {
		t.Fatal("retry depended on retrieval", err)
	}
	failing, err := facts.NewWithRetrieval(failedContextLedger{l}, allow{}, store, model.RetrievalOptions{Index: index, Progress: l, Generation: "g1"})
	if err != nil {
		t.Fatal(err)
	}
	bad := r
	bad.Key = "context-rollback"
	empty, err := failing.PrepareContext(ctx, c, bad)
	if !errors.Is(err, model.ErrUnavailable) || len(empty.Rendered) != 0 {
		t.Fatal("returned uncommitted context", err)
	}
	testRemoteContextFailure(t, ctx, failing, c, bad)
	artifact := func(id string) model.ArtifactReference {
		ref, err := store.Put(ctx, c.Tenant, id, []byte(id))
		if err != nil {
			t.Fatal(err)
		}
		_, err = e.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: model.Command{Key: id, Purpose: q.Purpose}, Evidence: model.Evidence{ID: id, Scope: "case1", SourceSystem: "app", SourceObject: id, SourceRevision: "v1", Speaker: "app", SourceClass: "organization_record", SourceTime: time.Now().UTC(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "decision-artifacts", Object: ref}})
		if err != nil {
			t.Fatal(err)
		}
		return model.ArtifactReference{EvidenceID: id, Digest: ref.Digest}
	}
	item := model.VersionReference{RecordID: m.Items[0].Fact.RecordID, VersionID: m.Items[0].Fact.VersionID}
	d := model.DecisionReceipt{Command: model.Command{Key: "decision-one", Purpose: q.Purpose}, Scope: "case1", ApplicationID: "app", ExternalDecisionID: "one", ContextID: m.ID, SelectedItems: []model.VersionReference{item}, UsedItems: []model.VersionReference{item}, MethodKind: "rule", MethodID: "rule1", MethodVersion: "v1", TransformationVersion: "v1", Input: artifact("decision-input"), Output: artifact("decision-output"), DecidedAt: time.Now(), Outcome: "succeeded"}
	testDecisionPayloadAtomicity(t, ctx, e, admin, c, d)
	receipt, err := remoteRecordDecision(t, ctx, e, c, d)
	if err != nil {
		t.Fatal(err)
	}
	d.Key = "decision-retry"
	again, err := e.RecordDecision(ctx, c, d)
	if err != nil || again.ID != receipt.ID {
		t.Fatal("decision retry", err)
	}
	d.Key = "decision-changed"
	d.Outcome = "failed"
	if _, err = e.RecordDecision(ctx, c, d); !errors.Is(err, model.ErrConflict) {
		t.Fatal("changed decision accepted", err)
	}
	d.ExternalDecisionID = "invalid"
	d.SelectedItems = nil
	if _, err = e.RecordDecision(ctx, c, d); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("used outside selected", err)
	}
	d.SelectedItems = []model.VersionReference{item}
	d.MethodKind = "model"
	if _, err = e.RecordDecision(ctx, c, d); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("missing model artifacts", err)
	}
	d.ModelRequest = &d.Input
	d.ModelResponse = &d.Output
	d.Key = "decision-model"
	d.ExternalDecisionID = "model"
	if _, err = remoteRecordDecision(t, ctx, e, c, d); err != nil {
		t.Fatal(err)
	}
	other := c
	other.Tenant = "org2"
	if _, err = e.GetContextManifest(ctx, other, model.ContextQuery{ID: m.ID, Purpose: q.Purpose}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross tenant context", err)
	}
	if _, err = e.GetDecisionReceipt(ctx, other, model.DecisionQuery{ID: receipt.ID, Purpose: q.Purpose}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross tenant decision", err)
	}
	snap, err := e.CreateFactSnapshot(ctx, c, model.FactSnapshotRequest{Command: model.Command{Key: "decision-snapshot", Purpose: q.Purpose}, Selectors: []model.FactKey{m.Items[0].Key}, ValidAt: q.ValidAt})
	if err != nil {
		t.Fatal(err)
	}
	d.Key = "decision-snapshot"
	d.ExternalDecisionID = "snapshot"
	d.ContextID = ""
	d.SnapshotID = snap.ID
	calculation, err := e.RecordCalculation(ctx, c, model.CalculationReceipt{Command: model.Command{Key: "decision-calculation", Purpose: q.Purpose}, Scope: "case1", SnapshotID: snap.ID, ApplicationID: "app", ExternalRunID: "decision-calculation", MethodID: "method", MethodVersion: "v1", ExecutableDigest: model.BytesDigest([]byte("executable")), ConfigurationDigest: model.BytesDigest([]byte("configuration")), NormalizationVersion: "v1", Input: d.Input, Output: d.Output, StartedAt: d.DecidedAt.Add(-time.Second), CompletedAt: d.DecidedAt, Outcome: "succeeded"})
	if err != nil {
		t.Fatal(err)
	}
	d.CalculationID = calculation.ID
	d.ActionReceipt = &d.Output
	if _, err = remoteRecordDecision(t, ctx, e, c, d); err != nil {
		t.Fatal(err)
	}
	denied, _ := facts.New(l, denyScope{scope: "case1"}, store)
	if _, err = denied.GetContextManifest(ctx, c, model.ContextQuery{ID: m.ID, Purpose: q.Purpose}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("revoked context access", err)
	}
	if _, err = denied.GetDecisionReceipt(ctx, c, model.DecisionQuery{ID: receipt.ID, Purpose: q.Purpose}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("revoked decision access", err)
	}
	testRemoteContextDenied(t, ctx, denied, c, m.ID, receipt.ID)
	var wg sync.WaitGroup
	results := make(chan model.DecisionRecord, 2)
	failures := make(chan error, 2)
	for _, key := range []string{"decision-race-a", "decision-race-b"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			request := d
			request.Key = key
			request.ExternalDecisionID = "race"
			rec, err := e.RecordDecision(ctx, c, request)
			results <- rec
			failures <- err
		}(key)
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	previous := ""
	for rec := range results {
		if previous != "" && previous != rec.ID {
			t.Fatal("duplicate decision")
		}
		previous = rec.ID
	}
	mixed := testMemoryContexts(t, ctx, e, l, store, c, index, d.Input, d.Output)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := audit.NewEd25519("context-audit-key", private)
	if err != nil {
		t.Fatal(err)
	}
	exporter, err := facts.NewWithAudit(l, allow{}, store, model.AuditOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	job, err := exporter.StartAuditExport(ctx, c, model.AuditExportRequest{Command: model.Command{Key: "context-export", Purpose: q.Purpose}, Scope: "case1"})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := exporter.RunAuditExport(ctx, c, model.AuditExportQuery{ID: job.ID, Scope: "case1", Purpose: q.Purpose})
	if err != nil {
		t.Fatal(err)
	}
	if err = audit.Verify(*completed.Bundle, map[string]ed25519.PublicKey{"context-audit-key": public}); err != nil {
		t.Fatal(err)
	}
	for _, entry := range completed.Bundle.Entries {
		if entry.Kind == "context" && entry.ID == mixed.ID {
			t.Fatal("memory body leaked into metadata export")
		}
	}
	foundGap := false
	for _, gap := range completed.Bundle.Manifest.Gaps {
		if gap.Kind == "context" && gap.ID == mixed.ID && gap.Reason == "memory_context_payload_withheld" {
			foundGap = true
		}
	}
	if !foundGap {
		t.Fatal("missing memory context redaction gap")
	}
	foundContext, foundDecision := false, false
	for _, entry := range completed.Bundle.Entries {
		if entry.Kind == "context" && entry.ID == m.ID {
			var retained model.ContextManifest
			if err = json.Unmarshal(entry.Payload, &retained); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(retained.Rendered, m.Rendered) {
				t.Fatal("export changed supplied bytes")
			}
			foundContext = true
		}
		if entry.Kind == "decision" && entry.ID == receipt.ID {
			foundDecision = true
		}
	}
	if !foundContext || !foundDecision {
		t.Fatal("missing context or decision in signed export")
	}

	fullJob, err := exporter.StartAuditExport(ctx, c, model.AuditExportRequest{Command: model.Command{Key: "memory-context-export", Purpose: q.Purpose}, Scope: "case1", IncludeEvidence: true})
	if err != nil {
		t.Fatal(err)
	}
	full, err := exporter.RunAuditExport(ctx, c, model.AuditExportQuery{ID: fullJob.ID, Scope: "case1", Purpose: q.Purpose})
	if err != nil {
		t.Fatal(err)
	}
	if err = audit.Verify(*full.Bundle, map[string]ed25519.PublicKey{"context-audit-key": public}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range full.Bundle.Entries {
		if entry.Kind == "context" && entry.ID == mixed.ID {
			var retained model.ContextManifest
			if err = json.Unmarshal(entry.Payload, &retained); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(retained.Rendered, mixed.Rendered) {
				t.Fatal("mixed context bytes changed in export")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("mixed context absent from full export")
	}

}
