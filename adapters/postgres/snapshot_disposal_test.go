package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testSnapshotDisposal(t *testing.T, ctx context.Context, normal *facts.Engine, store *evidence.Postgres, admin *pgxpool.Pool) {
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
	_, private, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := audit.NewEd25519("snapshot-key", private)
	engine, err := facts.NewWithAudit(ledger, allow{}, store, model.AuditOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	c := model.Caller{Tenant: "snapshot-retention", Principal: "proposer"}
	reviewer := c
	reviewer.Principal = "reviewer"
	command := func(key string) model.Command { return model.Command{Key: key, Purpose: "snapshot-retention"} }
	now := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	secret := "snapshot copied value"
	keys := []model.FactKey{}
	for _, scope := range []string{"case1", "case2"} {
		registerTestIdentities(t, ctx, normal, c.Tenant, scope, []string{"subject"})
		if _, err := normal.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: command(scope + "-acceptance"), Policy: model.AcceptancePolicy{Scope: scope, ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := normal.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: command(scope + "-predicate"), Definition: model.PredicateVersion{Scope: scope, Predicate: "value", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
			t.Fatal(err)
		}
		ref, err := store.Put(ctx, c.Tenant, scope+"-source", []byte(secret))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := normal.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: command(scope + "-evidence"), Evidence: model.Evidence{ID: scope + "-source", Scope: scope, SourceSystem: "test", SourceObject: scope, SourceRevision: "v1", Speaker: "customer", SourceClass: "customer", SourceTime: now, MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "source-policy", Object: ref}}); err != nil {
			t.Fatal(err)
		}
		key := model.FactKey{Scope: scope, Subject: "subject", Predicate: "value"}
		keys = append(keys, key)
		proposed, err := normal.ProposeFact(ctx, c, model.FactProposal{Command: command(scope + "-propose"), Key: key, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &secret}, Valid: model.Interval{From: &now}, Sources: []model.SourceSpan{{EvidenceID: scope + "-source", Start: 0, End: len(secret), Quote: secret}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := normal.ApproveFact(ctx, reviewer, model.Approval{Command: command(scope + "-approve"), Scope: scope, RecordID: proposed.RecordID, VersionID: proposed.VersionID, ExpectedRevision: proposed.StreamRevision}); err != nil {
			t.Fatal(err)
		}
		for _, policy := range []model.RetentionPolicy{{ID: "snapshots", Scope: scope, RecordClass: "snapshot_payload", Purpose: "snapshot-retention", Jurisdiction: "SG"}, {ID: "young", Scope: scope, RecordClass: "snapshot_payload", Purpose: "snapshot-retention", Jurisdiction: "SG", MinimumAgeSeconds: 3600}} {
			if _, err := engine.RegisterRetentionPolicy(ctx, c, model.RetentionPolicyRequest{Command: command(scope + policy.ID), Policy: policy}); err != nil {
				t.Fatal(err)
			}
		}
	}
	snapshot := func(id string, selectors []model.FactKey) model.FactSnapshot {
		t.Helper()
		s, err := normal.CreateFactSnapshot(ctx, c, model.FactSnapshotRequest{Command: command(id), Selectors: selectors, ValidAt: now})
		if err != nil {
			t.Fatal(err)
		}
		if s.Items[0].Resolution.Value == nil {
			t.Fatal("snapshot fixture lacks copied content")
		}
		return s
	}
	one := snapshot("one", keys[:1])
	multi := snapshot("multi", keys)
	calculation := snapshot("calculation", keys[:1])
	decision := snapshot("decision", keys[:1])
	exported := snapshot("exported", keys[:1])
	request := func(s model.FactSnapshot) model.SnapshotDisposalRequest {
		r := model.SnapshotDisposalRequest{Command: command("dispose-" + s.ID), SnapshotID: s.ID, Reason: "retention period expired"}
		for _, item := range s.Items {
			r.Bindings = append(r.Bindings, model.SnapshotRetentionBinding{Scope: item.Selector.Scope, PolicyID: "snapshots", Jurisdiction: "SG"})
		}
		return r
	}
	reject := func(e *facts.Engine, r model.SnapshotDisposalRequest, want error) {
		t.Helper()
		if _, err := e.DisposeSnapshot(ctx, c, r); !errors.Is(err, want) {
			t.Fatalf("snapshot disposal got %v want %v", err, want)
		}
	}
	reject(normal, request(one), model.ErrDenied)
	bad := request(one)
	bad.Bindings[0].PolicyID = "young"
	reject(engine, bad, model.ErrPrecondition)
	bad = request(multi)
	bad.Bindings = bad.Bindings[:1]
	reject(engine, bad, model.ErrDenied)
	bad = request(one)
	bad.SnapshotID = "missing-snapshot"
	reject(engine, bad, model.ErrDenied)
	bad = request(one)
	bad.Bindings[0].Scope = "case2"
	reject(engine, bad, model.ErrDenied)
	denied, _ := facts.New(ledger, denyScope{scope: "case2"}, store)
	reject(denied, request(multi), model.ErrDenied)
	hold := model.LegalHoldRequest{Command: command("hold"), ID: "snapshot-hold", Scope: "case2", Reason: "review"}
	if _, err := engine.PlaceLegalHold(ctx, c, hold); err != nil {
		t.Fatal(err)
	}
	reject(engine, request(multi), model.ErrPrecondition)
	if _, err := engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: command("release"), ID: hold.ID, Scope: hold.Scope, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,'archive-pending',3)", c.Tenant); err != nil {
		t.Fatal(err)
	}
	reject(engine, request(one), model.ErrPrecondition)
	if _, err := admin.Exec(ctx, "DELETE FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id='archive-pending'", c.Tenant); err != nil {
		t.Fatal(err)
	}
	calc := recordCalculationFixture(t, ctx, normal, store, c, calculation)
	disposedCalculation := testCalculationDisposal(t, ctx, normal, engine, ledger, admin, c, calc.Receipt, []model.FactSnapshot{snapshot("calc-batch-one", keys[:1]), snapshot("calc-batch-multi", keys)})
	reject(engine, request(calculation), model.ErrPrecondition)
	used := model.VersionReference{RecordID: decision.Items[0].Resolution.RecordID, VersionID: decision.Items[0].Resolution.VersionID}
	if _, err := normal.RecordDecision(ctx, c, model.DecisionReceipt{Command: command("decision-record"), Scope: "case1", ApplicationID: "app", ExternalDecisionID: "decision", SnapshotID: decision.ID, SelectedItems: []model.VersionReference{used}, UsedItems: []model.VersionReference{used}, MethodKind: "rule", MethodID: "rule", MethodVersion: "v1", TransformationVersion: "v1", Input: calc.Receipt.Input, Output: calc.Receipt.Output, DecidedAt: time.Now().UTC(), Outcome: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	reject(engine, request(decision), model.ErrPrecondition)
	raced := snapshot("raced", keys[:1])
	raceRequest := calc.Receipt
	raceRequest.Key = "raced-calculation"
	raceRequest.ExternalRunID = "raced-calculation"
	raceRequest.SnapshotID = raced.ID
	start := make(chan struct{})
	calculated := make(chan error, 1)
	go func() { <-start; _, err := normal.RecordCalculation(ctx, c, raceRequest); calculated <- err }()
	close(start)
	_, disposeErr := engine.DisposeSnapshot(ctx, c, request(raced))
	calculationErr := <-calculated
	if (disposeErr == nil) == (calculationErr == nil) {
		t.Fatal("snapshot reference race had wrong winners", disposeErr, calculationErr)
	}
	loser := disposeErr
	if loser == nil {
		loser = calculationErr
	}
	if !errors.Is(loser, model.ErrPrecondition) {
		t.Fatal("snapshot race error", loser)
	}
	failing, _ := facts.New(rollbackRetentionLedger{ledger}, allow{}, store)
	reject(failing, request(one), model.ErrUnavailable)
	if _, err := normal.GetFactSnapshot(ctx, c, model.SnapshotQuery{ID: one.ID, Purpose: "read"}); err != nil {
		t.Fatal("rollback lost snapshot", err)
	}
	var original []byte
	if err := admin.QueryRow(ctx, "SELECT payload FROM fact_snapshot_payloads WHERE tenant_id=$1 AND snapshot_id=$2", c.Tenant, one.ID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	verifyArchiveReplay := archiveSnapshotsBeforeDisposal(t, ctx, admin, cipher, signer, map[string]ed25519.PublicKey{"snapshot-key": private.Public().(ed25519.PublicKey)}, c.Tenant, one, multi)
	for _, s := range []model.FactSnapshot{one, multi} {
		r := request(s)
		first, err := engine.DisposeSnapshot(ctx, c, r)
		if err != nil {
			t.Fatal(err)
		}
		testSnapshotDisposalRemote(t, ctx, engine, c, r, first)
		retry, err := engine.DisposeSnapshot(ctx, c, r)
		if err != nil || retry != first {
			t.Fatal("snapshot replay", retry, err)
		}
		r.Reason = "changed"
		reject(engine, r, model.ErrConflict)
		got, err := normal.GetFactSnapshot(ctx, c, model.SnapshotQuery{ID: s.ID, Purpose: "read"})
		if !errors.Is(err, model.ErrPrecondition) || len(got.Items) != 0 {
			t.Fatal("disposed snapshot disclosed", got, err)
		}
		var count int
		if err := admin.QueryRow(ctx, "SELECT count(*) FROM fact_snapshot_payloads WHERE tenant_id=$1 AND snapshot_id=$2", c.Tenant, s.ID).Scan(&count); err != nil || count != 0 {
			t.Fatal("snapshot bytes retained", count, err)
		}
		if err := ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
			metadata, err := tx.(model.TemporalTx).Snapshot(ctx, s.ID)
			if err != nil {
				return err
			}
			if !metadata.PayloadDisposed || metadata.Digest != s.Digest || metadata.Items[0].Resolution.Value != nil {
				t.Fatal("disposal manifest", metadata)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	verifyArchiveReplay()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := ledgerdb.New(tx).BindTenant(ctx, c.Tenant); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "INSERT INTO fact_snapshot_payloads(tenant_id,snapshot_id,payload) VALUES($1,$2,$3)", c.Tenant, one.ID, original)
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "23514" {
		t.Fatal("snapshot resurrected", err)
	}
	_ = tx.Rollback(ctx)
	job, err := engine.StartAuditExport(ctx, c, model.AuditExportRequest{Command: command("export"), Scope: "case1", IncludeEvidence: true})
	if err != nil {
		t.Fatal(err)
	}
	reject(engine, request(exported), model.ErrPrecondition)
	result, err := engine.RunAuditExport(ctx, c, model.AuditExportQuery{ID: job.ID, Scope: "case1", Purpose: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	calculationGap := false
	for _, g := range result.Bundle.Manifest.Gaps {
		if g.Kind == "calculation_content" && g.ID == disposedCalculation {
			calculationGap = true
		}
	}
	if !calculationGap {
		t.Fatal("calculation export gap missing")
	}
	gap := false
	for _, g := range result.Bundle.Manifest.Gaps {
		if g.Kind == "snapshot_content" && g.ID == one.ID {
			gap = true
		}
	}
	if !gap {
		t.Fatal("snapshot export gap missing")
	}
	for _, entry := range result.Bundle.Entries {
		if entry.Kind == "snapshot" {
			var s model.FactSnapshot
			if err := json.Unmarshal(entry.Payload, &s); err != nil {
				t.Fatal(err)
			}
			if s.ID == multi.ID {
				t.Fatal("cross-scope tombstone disclosed")
			}
			if s.ID == one.ID && (!s.PayloadDisposed || s.Items[0].Resolution.Value != nil) {
				t.Fatal("export reconstructed disposed bytes")
			}
		}
	}
	changes, err := normal.ListChanges(ctx, c, model.ChangesQuery{Scope: "case1", Purpose: "archive", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range changes.Events {
		if event.LedgerRevision == one.CreationRevision {
			payload, err := ledger.ArchivePayload(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant, Consumer: "archive-fixture"}, Event: event})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, g := range payload.Gaps {
				if g.Kind == "snapshot_content" && g.ID == one.ID {
					found = true
				}
			}
			if !found {
				t.Fatal("archive snapshot gap missing")
			}
			return
		}
	}
	t.Fatal("snapshot creation event missing")
}
