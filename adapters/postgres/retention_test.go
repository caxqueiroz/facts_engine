package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
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

type rollbackRetentionLedger struct{ *postgres.Ledger }
type rollbackRetentionTx struct{ model.RetentionTx }

func (t rollbackRetentionTx) Append(context.Context, model.Event) (model.Event, error) {
	return model.Event{}, model.ErrUnavailable
}
func (l rollbackRetentionLedger) Transact(ctx context.Context, tenant string, fn func(model.Tx) error) error {
	return l.Ledger.Transact(ctx, tenant, func(tx model.Tx) error { return fn(rollbackRetentionTx{tx.(model.RetentionTx)}) })
}
func testRetention(t *testing.T, ctx context.Context, normal *facts.Engine, store *evidence.Postgres, admin *pgxpool.Pool) {
	t.Helper()
	cfg := admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["role"] = "facts_retention"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := facts.New(ledger, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	c := model.Caller{Tenant: "retention-bank", Principal: "retention-officer"}
	command := func(key string) model.Command { return model.Command{Key: key, Purpose: "test-disposal"} }
	policy := model.RetentionPolicyRequest{Command: command("retention-policy"), Policy: model.RetentionPolicy{ID: "short-lived", Scope: "case", RecordClass: "evidence_bytes", Purpose: "test-disposal", Jurisdiction: "SG", MinimumAgeSeconds: 0}}
	if _, err = normal.RegisterRetentionPolicy(ctx, c, policy); !errors.Is(err, model.ErrDenied) {
		t.Fatal("runtime wrote retention policy", err)
	}
	if _, err = engine.RegisterRetentionPolicy(ctx, c, policy); err != nil {
		t.Fatal(err)
	}
	register := func(id, object, scope, policy string) model.ObjectRef {
		t.Helper()
		ref, err := store.Put(ctx, c.Tenant, object, []byte(object))
		if err != nil {
			t.Fatal(err)
		}
		_, err = normal.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: command(id), Evidence: model.Evidence{ID: id, Scope: scope, SourceSystem: "test", SourceObject: id, SourceRevision: "v1", Speaker: "staff", SourceClass: "bank_record", SourceTime: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: policy, Object: ref}})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	ref := register("disposable", "disposable-object", "case", "short-lived")
	request := model.EvidenceDisposalRequest{Command: command("dispose"), Scope: "case", EvidenceID: "disposable", PolicyID: "short-lived", Jurisdiction: "SG", Reason: "schedule-expired"}
	hold := model.LegalHoldRequest{Command: command("hold"), ID: "hold1", Scope: "case", Reason: "legal-review"}
	if _, err = engine.PlaceLegalHold(ctx, c, hold); err != nil {
		t.Fatal(err)
	}
	if _, err = engine.DisposeEvidence(ctx, c, request); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("hold bypassed", err)
	}
	if _, err = normal.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: command("release"), ID: hold.ID, Scope: hold.Scope, Reason: "authorized"}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("runtime released hold", err)
	}
	if _, err = engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: command("release"), ID: hold.ID, Scope: hold.Scope, Reason: "authorized"}); err != nil {
		t.Fatal(err)
	}
	failing, _ := facts.New(rollbackRetentionLedger{ledger}, allow{}, store)
	if _, err = failing.DisposeEvidence(ctx, c, request); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("failure injection", err)
	}
	if _, err = store.Read(ctx, c.Tenant, ref); err != nil {
		t.Fatal("rollback lost bytes", err)
	}
	result, err := engine.DisposeEvidence(ctx, c, request)
	if err != nil {
		t.Fatal(err)
	}
	again, err := engine.DisposeEvidence(ctx, c, request)
	if err != nil || again != result {
		t.Fatal("disposal retry", err)
	}
	if _, err = store.Read(ctx, c.Tenant, ref); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("disposed bytes readable", err)
	}
	if _, err = store.Put(ctx, c.Tenant, "disposable-object", []byte("disposable-object")); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("resurrected disposed object", err)
	}
	status, err := engine.GetRetentionStatus(ctx, c, model.RetentionStatusQuery{Scope: "case", EvidenceID: "disposable", Purpose: "review"})
	if err != nil || !status.Disposed || len(status.ActiveHolds) != 0 {
		t.Fatal(status, err)
	}
	other := c
	other.Tenant = "other-bank"
	if _, err = engine.GetRetentionStatus(ctx, other, model.RetentionStatusQuery{Scope: "case", EvidenceID: "disposable", Purpose: "review"}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross tenant disposal status", err)
	}
	policy.Key = "long-policy"
	policy.Policy.ID = "long-lived"
	policy.Policy.MinimumAgeSeconds = 86400
	if _, err = engine.RegisterRetentionPolicy(ctx, c, policy); err != nil {
		t.Fatal(err)
	}
	register("too-young", "young-object", "case", "long-lived")
	young := request
	young.Key = "young-disposal"
	young.EvidenceID = "too-young"
	young.PolicyID = "long-lived"
	if _, err = engine.DisposeEvidence(ctx, c, young); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("used historical source date for retention age", err)
	}
	register("shared-a", "shared-object", "case", "short-lived")
	register("shared-b", "shared-object", "other-scope", "short-lived")
	shared := request
	shared.Key = "shared-disposal"
	shared.EvidenceID = "shared-a"
	if _, err = engine.DisposeEvidence(ctx, c, shared); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("shared object disposed", err)
	}
	register("referenced", "referenced-object", "case", "short-lived")
	_, err = normal.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: command("fact-policy"), Policy: model.AcceptancePolicy{ID: "manual", Scope: "case", Mode: "manual", SourceClasses: []string{"bank_record"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = normal.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: command("fact-schema"), Definition: model.PredicateVersion{Scope: "case", Predicate: "test", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}})
	if err != nil {
		t.Fatal(err)
	}
	value := "referenced"
	proposal := model.FactProposal{Command: command("fact"), Key: model.FactKey{Scope: "case", Subject: "subject", Predicate: "test"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Sources: []model.SourceSpan{{EvidenceID: "referenced", Start: 0, End: 10, Quote: "referenced"}}}
	if _, err = normal.ProposeFact(ctx, c, proposal); err != nil {
		t.Fatal(err)
	}
	linked := request
	linked.Key = "linked-disposal"
	linked.EvidenceID = "referenced"
	if _, err = engine.DisposeEvidence(ctx, c, linked); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("referenced object disposed", err)
	}
	// A proposal that validated bytes before disposal still cannot append a new
	// reference afterwards: the ledger guard is independent of service validation.
	err = ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error { return tx.InsertAttestation(ctx, model.Attestation{EvidenceID: "disposable"}) })
	if !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("stale validation bypassed tombstone", err)
	}

	// Force the disposal transaction to establish an old repeatable-read view.
	// A concurrent legal hold must force a retry, then block disposal.
	register("hold-race", "hold-race-object", "case", "short-lived")
	raceRequest := request
	raceRequest.Key = "race-disposal"
	raceRequest.EvidenceID = "hold-race"
	ready, resume := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	var once sync.Once
	go func() {
		finished <- ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
			rt := tx.(model.RetentionTx)
			if _, err := rt.RetentionStatus(ctx, "case", "hold-race"); err != nil {
				return err
			}
			once.Do(func() { close(ready); <-resume })
			return rt.DisposeEvidence(ctx, raceRequest)
		})
	}()
	<-ready
	_, holdErr := engine.PlaceLegalHold(ctx, c, model.LegalHoldRequest{Command: command("concurrent-hold"), ID: "race-hold", Scope: "case", Reason: "litigation"})
	close(resume)
	raceErr := <-finished
	if holdErr != nil {
		t.Fatal(holdErr)
	}
	if !errors.Is(raceErr, model.ErrPrecondition) {
		t.Fatal("stale transaction bypassed concurrent hold", raceErr)
	}
	if _, err = engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: command("release-race"), ID: "race-hold", Scope: "case", Reason: "closed"}); err != nil {
		t.Fatal(err)
	}
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := audit.NewEd25519("retention-key", private)
	exporter, _ := facts.NewWithAudit(ledger, allow{}, store, model.AuditOptions{Signer: signer})
	job, err := exporter.StartAuditExport(ctx, c, model.AuditExportRequest{Command: command("retention-export"), Scope: "case", IncludeEvidence: true})
	if err != nil {
		t.Fatal(err)
	}
	done, err := exporter.RunAuditExport(ctx, c, model.AuditExportQuery{ID: job.ID, Scope: "case", Purpose: "review"})
	if err != nil {
		t.Fatal(err)
	}
	if err = audit.Verify(*done.Bundle, map[string]ed25519.PublicKey{"retention-key": public}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, gap := range done.Bundle.Manifest.Gaps {
		if gap.ID == "disposable" && gap.Reason == "disposed_under_retention_policy" {
			found = true
		}
	}
	if !found {
		t.Fatal("missing disposal coverage gap")
	}
	register("export-blocked", "export-blocked-object", "case", "short-lived")
	blocked := request
	blocked.Key = "export-blocked"
	blocked.EvidenceID = "export-blocked"
	if _, err = engine.DisposeEvidence(ctx, c, blocked); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("export inventory bypassed", err)
	}
}
