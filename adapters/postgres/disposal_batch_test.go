package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDisposalBatch(t *testing.T, ctx context.Context, normal, engine, runner *facts.Engine, ledger *postgres.Ledger, admin *pgxpool.Pool, c model.Caller, job model.JobStatus, proposal model.ExtractedProposal, extraction model.ExtractionDisposalRequest) string {
	t.Helper()
	cmd := func(key string) model.Command { return model.Command{Key: "batch-" + key, Purpose: "retention"} }
	for _, p := range []model.RetentionPolicy{{ID: "batch-fact", Scope: job.Scope, RecordClass: "fact_payload", Purpose: "retention", Jurisdiction: "SG"}, {ID: "batch-young", Scope: job.Scope, RecordClass: "fact_payload", Purpose: "retention", Jurisdiction: "SG", MinimumAgeSeconds: 3600}, {ID: "batch-snapshot", Scope: job.Scope, RecordClass: "snapshot_payload", Purpose: "retention", Jurisdiction: "SG"}} {
		if _, err := engine.RegisterRetentionPolicy(ctx, c, model.RetentionPolicyRequest{Command: cmd(p.ID), Policy: p}); err != nil {
			t.Fatal(err)
		}
	}
	extraction.Command = model.Command{}
	extraction.Reason = ""
	r := model.DisposalBatchRequest{Command: cmd("dispose"), Scope: job.Scope, Reason: "expired", Extractions: []model.ExtractionDisposalRequest{extraction}, Versions: []model.VersionDisposalRequest{{Kind: "fact", Scope: job.Scope, RecordID: proposal.RecordID, VersionID: proposal.VersionID, Bindings: []model.VersionRetentionBinding{{Scope: job.Scope, PolicyID: "batch-fact", Jurisdiction: "SG"}}}}}
	reject := func(e *facts.Engine, r model.DisposalBatchRequest, want error) {
		t.Helper()
		if _, err := e.DisposeBatch(ctx, c, r); !errors.Is(err, want) {
			t.Fatal("batch rejection", err, want)
		}
	}
	retained := func() {
		t.Helper()
		run, err := runner.GetExtractionRun(ctx, c, model.ExtractionRunQuery{ID: job.AttemptID, Scope: job.Scope, Purpose: "read"})
		if err != nil || run.Result == nil || len(run.Result.Proposals) != 1 {
			t.Fatal("failed batch lost extraction copy", run, err)
		}
		if err = ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error { _, err := tx.Version(ctx, proposal.RecordID, proposal.VersionID); return err }); err != nil {
			t.Fatal("failed batch lost version", err)
		}
	}
	reject(normal, r, model.ErrDenied)
	partial := r
	partial.Versions = nil
	reject(engine, partial, model.ErrPrecondition)
	partial = r
	partial.Extractions = nil
	reject(engine, partial, model.ErrPrecondition)
	bad := r
	bad.Versions = append([]model.VersionDisposalRequest(nil), r.Versions...)
	bad.Versions[0].Bindings = []model.VersionRetentionBinding{{Scope: job.Scope, PolicyID: "batch-young", Jurisdiction: "SG"}}
	reject(engine, bad, model.ErrPrecondition)
	retained()
	if _, err := engine.PlaceLegalHold(ctx, c, model.LegalHoldRequest{Command: cmd("hold"), ID: "batch-hold", Scope: job.Scope, Reason: "review"}); err != nil {
		t.Fatal(err)
	}
	reject(engine, r, model.ErrPrecondition)
	if _, err := engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: cmd("release"), ID: "batch-hold", Scope: job.Scope, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "REVOKE DELETE ON fact_version_payloads FROM facts_retention"); err != nil {
		t.Fatal(err)
	}
	reject(engine, r, model.ErrDenied)
	if _, err := admin.Exec(ctx, "GRANT DELETE ON fact_version_payloads TO facts_retention"); err != nil {
		t.Fatal(err)
	}
	retained()
	reviewer := c
	reviewer.Principal = "batch-reviewer"
	if _, err := normal.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("approve"), Scope: job.Scope, RecordID: proposal.RecordID, VersionID: proposal.VersionID, ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := normal.CreateFactSnapshot(ctx, c, model.FactSnapshotRequest{Command: cmd("snapshot"), Selectors: []model.FactKey{{Scope: job.Scope, Subject: "subject", Predicate: "value"}}, ValidAt: time.Now().UTC()})
	if err != nil || len(snapshot.Items) != 1 || snapshot.Items[0].Resolution.Value == nil {
		t.Fatal(snapshot, err)
	}
	reject(engine, r, model.ErrPrecondition)
	retained()
	r.Snapshots = []model.SnapshotDisposalRequest{{SnapshotID: snapshot.ID, Bindings: []model.SnapshotRetentionBinding{{Scope: job.Scope, PolicyID: "batch-snapshot", Jurisdiction: "SG"}}}}
	bad = r
	bad.Versions = append([]model.VersionDisposalRequest(nil), r.Versions...)
	bad.Versions[0].Bindings = []model.VersionRetentionBinding{{Scope: job.Scope, PolicyID: "batch-young", Jurisdiction: "SG"}}
	reject(engine, bad, model.ErrPrecondition)
	retained()
	if err = ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		got, err := tx.(model.TemporalTx).Snapshot(ctx, snapshot.ID)
		if err == nil && (got.PayloadDisposed || got.Items[0].Resolution.Value == nil) {
			t.Fatal("failed batch lost snapshot")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,$2,3)", c.Tenant, model.ProjectionConsumer("batch-index")); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO projection_destinations(tenant_id,generation,consumer_id) VALUES($1,'batch-index',$2)", c.Tenant, model.ProjectionConsumer("batch-index")); err != nil {
		t.Fatal(err)
	}
	first, err := engine.DisposeBatch(ctx, c, r)
	if err != nil {
		t.Fatal("complete selection rejected", err)
	}
	replay, err := engine.DisposeBatch(ctx, c, r)
	if err != nil || replay != first {
		t.Fatal(replay, err)
	}
	testDisposalBatchRemote(t, ctx, engine, c, r, first)
	bad = r
	bad.Reason = "changed"
	reject(engine, bad, model.ErrConflict)
	if _, err = runner.GetExtractionRun(ctx, c, model.ExtractionRunQuery{ID: job.AttemptID, Scope: job.Scope, Purpose: "read"}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("batch extraction still readable", err)
	}
	if err = ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error { _, err := tx.Version(ctx, proposal.RecordID, proposal.VersionID); return err }); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("batch version still readable", err)
	}
	var revision, events int
	if err = admin.QueryRow(ctx, "SELECT stream_revision,(SELECT count(*) FROM record_events e WHERE e.tenant_id=r.tenant_id AND e.record_id=r.id) FROM records r WHERE tenant_id=$1 AND id=$2", c.Tenant, proposal.RecordID).Scan(&revision, &events); err != nil || revision != 3 || events != 3 {
		t.Fatal("batch record history incomplete", revision, events, err)
	}
	pending, err := ledger.ScanProjectionCleanup(ctx, model.ProjectionCleanupQuery{Tenant: c.Tenant, Limit: 100})
	if err != nil || len(pending) != 1 || pending[0].VersionID != proposal.VersionID || pending[0].DisposalEventID == first.ID || pending[0].Purpose != r.Purpose {
		t.Fatal("batch cleanup authorization missing", pending, err)
	}
	return first.ID
}

func testVersionBatch(t *testing.T, ctx context.Context, normal, engine *facts.Engine, admin *pgxpool.Pool, c model.Caller, now time.Time, secret string) {
	t.Helper()
	cmd := func(key string) model.Command {
		return model.Command{Key: "version-batch-" + key, Purpose: "retention"}
	}
	if _, err := normal.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: cmd("predicate"), Definition: model.PredicateVersion{Scope: "a", Predicate: "batch-value", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	fact, err := normal.ProposeFact(ctx, c, model.FactProposal{Command: cmd("fact"), Key: model.FactKey{Scope: "a", Subject: "subject", Predicate: "batch-value"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &secret}, Valid: model.Interval{From: &now}, Sources: []model.SourceSpan{{EvidenceID: "a-source", Start: 0, End: len(secret), Quote: secret}}})
	if err != nil {
		t.Fatal(err)
	}
	reviewer := c
	reviewer.Principal = "batch-reviewer"
	if _, err = normal.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("approve"), Scope: "a", RecordID: fact.RecordID, VersionID: fact.VersionID, ExpectedRevision: fact.StreamRevision}); err != nil {
		t.Fatal(err)
	}
	memory, err := normal.ProposeMemory(ctx, c, model.MemoryProposal{Command: cmd("memory"), Key: model.MemoryKey{Scope: "z", ID: "batch-summary"}, Subtype: "summary", SchemaVersion: "v1", Body: secret, Valid: model.Interval{From: &now}, Sources: []model.SourceSpan{{EvidenceID: "z-source", Start: 0, End: len(secret), Quote: secret}}, Dependencies: []model.MemoryDependency{{Scope: "a", RecordID: fact.RecordID, VersionID: fact.VersionID, ContentDigest: fact.ContentDigest, Label: "assertion"}}})
	if err != nil {
		t.Fatal(err)
	}
	r := model.DisposalBatchRequest{Command: cmd("dispose"), Scope: "z", Reason: "expired", Versions: []model.VersionDisposalRequest{
		{Kind: "fact", Scope: "a", RecordID: fact.RecordID, VersionID: fact.VersionID, Bindings: []model.VersionRetentionBinding{{Scope: "a", PolicyID: "fact", Jurisdiction: "SG"}}},
		{Kind: "memory", Scope: "z", RecordID: memory.RecordID, VersionID: memory.VersionID, Bindings: []model.VersionRetentionBinding{{Scope: "z", PolicyID: "memory", Jurisdiction: "SG"}, {Scope: "a", PolicyID: "memory", Jurisdiction: "SG"}}},
	}}
	partial := r
	partial.Versions = r.Versions[:1]
	partial.Scope = "a"
	if _, err = engine.DisposeBatch(ctx, c, partial); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("unselected memory copy ignored", err)
	}
	partial = r
	partial.Versions = append([]model.VersionDisposalRequest(nil), r.Versions...)
	partial.Versions[1].Bindings = r.Versions[1].Bindings[:1]
	if _, err = engine.DisposeBatch(ctx, c, partial); !errors.Is(err, model.ErrDenied) {
		t.Fatal("missing dependency scope authorized", err)
	}
	first, err := engine.DisposeBatch(ctx, c, r)
	if err != nil {
		t.Fatal(err)
	}
	r.Versions[0], r.Versions[1] = r.Versions[1], r.Versions[0]
	replay, err := engine.DisposeBatch(ctx, c, r)
	if err != nil || replay != first {
		t.Fatal("version batch reordered replay", replay, err)
	}
	testDisposalBatchRemote(t, ctx, engine, c, r, first)
	if _, err = normal.GetMemoryVersion(ctx, c, model.MemoryVersionQuery{Scope: "z", RecordID: memory.RecordID, VersionID: memory.VersionID, Purpose: "read"}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("memory body retained", err)
	}
	for _, item := range []struct {
		record, table string
		revision      int
	}{{fact.RecordID, "record_events", 3}, {memory.RecordID, "memory_record_events", 2}} {
		var revision, count int
		if err = admin.QueryRow(ctx, "SELECT stream_revision,(SELECT count(*) FROM "+item.table+" e WHERE e.tenant_id=r.tenant_id AND e.record_id=r.id) FROM records r WHERE r.tenant_id=$1 AND r.id=$2", c.Tenant, item.record).Scan(&revision, &count); err != nil || revision != item.revision || count != item.revision {
			t.Fatal("batch history discontinuity", revision, count, err)
		}
	}
}
