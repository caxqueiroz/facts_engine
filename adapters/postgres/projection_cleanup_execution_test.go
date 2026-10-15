package postgres_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

type cleanupExecutionIndex struct {
	*cortex.Generations
	calls     int
	failAfter bool
	after     func()
	admins    map[string]*sql.DB
}

func (i *cleanupExecutionIndex) DisposeProjection(ctx context.Context, r model.ProjectionDisposal) error {
	i.calls++
	if err := i.Generations.DisposeProjection(ctx, r); err != nil {
		return err
	}
	if i.after != nil {
		i.after()
	}
	if i.failAfter {
		i.failAfter = false
		return model.ErrUnavailable
	}
	return nil
}
func setupVersionCleanup(t *testing.T, ctx context.Context, admin *pgxpool.Pool, c model.Caller, fact, memory model.CommitResult, secret string) *cleanupExecutionIndex {
	t.Helper()
	if os.Getenv("TEST_CLEANUP_DATABASE_URL") == "" {
		return nil
	}
	indexes := []*cortex.Index{}
	out := &cleanupExecutionIndex{admins: map[string]*sql.DB{}}
	for _, spec := range []struct{ generation, env string }{{"static", "TEST_CLEANUP_DATABASE_URL"}, {"terminal", "TEST_CLEANUP_TERMINAL_DATABASE_URL"}} {
		dsn := os.Getenv(spec.env)
		if dsn == "" {
			t.Fatal("missing cleanup index fixture", spec.env)
		}
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		db.SetMaxOpenConns(4)
		config := cortex.Config{Tenant: c.Tenant, Generation: spec.generation, EmbeddingID: "test-constant-v1", Dimensions: 3}
		if err = cortex.Provision(ctx, db, config); err != nil {
			t.Fatal(err)
		}
		grants, err := os.ReadFile("../../scripts/cortex-runtime-role.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, string(grants)); err != nil {
			t.Fatal(err)
		}
		writerDB, err := sql.Open("pgx", databaseRole(t, dsn, "facts_cortex_runtime"))
		if err != nil {
			t.Fatal(err)
		}
		writerDB.SetMaxOpenConns(4)
		t.Cleanup(func() { _ = writerDB.Close() })
		writer, err := cortex.New(ctx, writerDB, config, pipelineEmbedder{})
		if err != nil {
			t.Fatal(err)
		}
		doc := model.ProjectionDocument{Tenant: c.Tenant, Generation: spec.generation, RecordID: fact.RecordID, VersionID: fact.VersionID, VersionDigest: fact.ContentDigest, PolicyID: "manual", Classification: "confidential", Key: model.FactKey{Scope: "a", Subject: "subject", Predicate: "value"}, Value: model.Value{Kind: model.Text, Text: &secret}}
		if err = writer.Put(ctx, doc); err != nil {
			t.Fatal(err)
		}
		doc.RecordID, doc.VersionID, doc.VersionDigest = memory.RecordID, memory.VersionID, memory.ContentDigest
		doc.Key, doc.Value = model.FactKey{}, model.Value{}
		doc.Memory = &model.MemoryProjection{Key: model.MemoryKey{Scope: "z", ID: "summary"}, Subtype: "summary", SchemaVersion: "v1", Body: secret}
		if err = writer.Put(ctx, doc); err != nil {
			t.Fatal(err)
		}
		retentionDB, err := sql.Open("pgx", databaseRole(t, dsn, "facts_cortex_retention"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = retentionDB.Close() })
		retentionDB.SetMaxOpenConns(4)
		index, err := cortex.New(ctx, retentionDB, config, pipelineEmbedder{})
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, index)
		out.admins[spec.generation] = db
		if spec.generation == "terminal" {
			if _, err = admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,$2,3)", c.Tenant, model.ProjectionConsumer(spec.generation)); err != nil {
				t.Fatal(err)
			}
			if _, err = admin.Exec(ctx, "INSERT INTO projection_generations(tenant_id,generation,state,fence) VALUES($1,$2,'abandoned',1)", c.Tenant, spec.generation); err != nil {
				t.Fatal(err)
			}
			if err = writer.SetProjectionGate(ctx, model.GenerationKey{Tenant: c.Tenant, Generation: spec.generation}, 1, true); err != nil {
				t.Fatal(err)
			}
		}
	}
	var err error
	out.Generations, err = cortex.NewGenerations(indexes...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func testVersionCleanupExecution(t *testing.T, ctx context.Context, ledger *postgres.Ledger, store *evidence.Postgres, admin *pgxpool.Pool, c model.Caller, index *cleanupExecutionIndex, f, m model.VersionDisposalRequest, factDisposal, memoryDisposal model.CommitResult) {
	t.Helper()
	engine, err := facts.NewWithOptions(ledger, allow{}, store, model.EngineOptions{ProjectionCleanup: model.ProjectionCleanupOptions{Destination: index}})
	if err != nil {
		t.Fatal(err)
	}
	request := func(v model.VersionDisposalRequest, event model.CommitResult, generation string) model.ProjectionCleanupRequest {
		return model.ProjectionCleanupRequest{Command: model.Command{Key: "cleanup-" + v.Kind + "-" + generation, Purpose: v.Purpose}, Kind: v.Kind, Scope: v.Scope, RecordID: v.RecordID, VersionID: v.VersionID, DisposalEventID: event.ID, Generation: generation, Reason: "expired", Bindings: append([]model.VersionRetentionBinding(nil), v.Bindings...)}
	}

	runtimeConfig := admin.Config().Copy()
	runtimeConfig.ConnConfig.RuntimeParams["role"] = "facts_runtime"
	runtimePool, err := pgxpool.NewWithConfig(ctx, runtimeConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer runtimePool.Close()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	runtimeLedger, err := postgres.New(ctx, runtimePool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	runtimeEngine, err := facts.NewWithOptions(runtimeLedger, allow{}, store, model.EngineOptions{ProjectionCleanup: model.ProjectionCleanupOptions{Destination: index}})
	if err != nil {
		t.Fatal(err)
	}
	r := request(m, memoryDisposal, "static")
	reject := func(e *facts.Engine, r model.ProjectionCleanupRequest, want error) {
		t.Helper()
		if _, err := e.CleanupProjection(ctx, c, r); !errors.Is(err, want) {
			t.Fatal("cleanup rejection", err, want)
		}
	}
	reject(runtimeEngine, r, model.ErrDenied)
	bad := r
	bad.DisposalEventID = "unknown"
	reject(engine, bad, model.ErrDenied)
	bad = r
	bad.Generation = "uncaptured"
	reject(engine, bad, model.ErrDenied)
	bad = r
	bad.RecordID = f.RecordID
	reject(engine, bad, model.ErrDenied)
	bad = r
	bad.Bindings = append([]model.VersionRetentionBinding(nil), r.Bindings[:1]...)
	reject(engine, bad, model.ErrDenied)
	denied, err := facts.NewWithOptions(ledger, denyScope{scope: "a"}, store, model.EngineOptions{ProjectionCleanup: model.ProjectionCleanupOptions{Destination: index}})
	if err != nil {
		t.Fatal(err)
	}
	reject(denied, r, model.ErrDenied)
	command := func(key string) model.Command { return model.Command{Key: key, Purpose: "retention"} }
	if _, err = engine.PlaceLegalHold(ctx, c, model.LegalHoldRequest{Command: command("cleanup-hold"), Scope: "a", ID: "cleanup-hold", Reason: "review"}); err != nil {
		t.Fatal(err)
	}
	reject(engine, r, model.ErrPrecondition)
	if _, err = engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: command("cleanup-release"), Scope: "a", ID: "cleanup-hold", Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	if index.calls != 0 {
		t.Fatal("blocked cleanup called destination", index.calls)
	}
	index.failAfter = true
	reject(engine, r, model.ErrUnavailable)
	status, err := ledger.ProjectionCleanupStatus(ctx, model.ProjectionCleanupKey{Tenant: c.Tenant, Kind: "memory", VersionID: m.VersionID})
	if err != nil || status.Complete || status.Targets[0].AcknowledgementID != "" {
		t.Fatal("failed call acknowledged", status, err)
	}
	first, err := engine.CleanupProjection(ctx, c, r)
	if err != nil {
		t.Fatal(err)
	}
	calls := index.calls
	r.Bindings[0], r.Bindings[1] = r.Bindings[1], r.Bindings[0]
	replay, err := engine.CleanupProjection(ctx, c, r)
	if err != nil || replay != first || index.calls != calls {
		t.Fatal("cleanup replay", replay, err, index.calls)
	}
	testCleanupProjectionRemote(t, ctx, engine, c, r, first)
	bad = r
	bad.Reason = "changed"
	reject(engine, bad, model.ErrConflict)
	// Removal can succeed before a ledger write fails. No acknowledgement may
	// survive that transaction, and an exact retry must finish safely.
	r = request(f, factDisposal, "static")
	index.after = func() {
		if _, err := admin.Exec(ctx, "REVOKE INSERT ON projection_cleanup_acknowledgements FROM facts_retention"); err != nil {
			t.Fatal(err)
		}
	}
	reject(engine, r, model.ErrDenied)
	index.after = nil
	if _, err = admin.Exec(ctx, "GRANT INSERT ON projection_cleanup_acknowledgements TO facts_retention"); err != nil {
		t.Fatal(err)
	}
	status, err = ledger.ProjectionCleanupStatus(ctx, model.ProjectionCleanupKey{Tenant: c.Tenant, Kind: "fact", VersionID: f.VersionID})
	if err != nil || status.Complete || status.Targets[0].AcknowledgementID != "" {
		t.Fatal("failed ledger commit acknowledged", status, err)
	}
	if _, err = engine.CleanupProjection(ctx, c, r); err != nil {
		t.Fatal("lost acknowledgement retry", err)
	}
	for _, pair := range []struct {
		v     model.VersionDisposalRequest
		event model.CommitResult
	}{{f, factDisposal}, {m, memoryDisposal}} {

		index.after = func() {
			held, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			_, holdErr := engine.PlaceLegalHold(held, c, model.LegalHoldRequest{Command: command("concurrent-cleanup-hold-" + pair.v.Kind), Scope: "a", ID: "concurrent-" + pair.v.Kind, Reason: "review"})
			if !errors.Is(holdErr, context.DeadlineExceeded) {
				t.Fatal("hold bypassed cleanup scope guard", holdErr)
			}
		}
		r = request(pair.v, pair.event, "terminal")
		if _, err = engine.CleanupProjection(ctx, c, r); err != nil {
			t.Fatal("terminal cleanup", err)
		}
		index.after = nil
		status, err = ledger.ProjectionCleanupStatus(ctx, model.ProjectionCleanupKey{Tenant: c.Tenant, Kind: r.Kind, VersionID: r.VersionID})
		if err != nil || !status.Complete || len(status.Targets) != 2 {
			t.Fatal("incomplete cleanup", status, err)
		}
		for _, target := range status.Targets {
			var count int
			if err = admin.QueryRow(ctx, "SELECT count(*) FROM retention_events WHERE tenant_id=$1 AND id=$2 AND kind='cleanup_projection'", c.Tenant, target.AcknowledgementID).Scan(&count); err != nil || count != 1 {
				t.Fatal("acknowledgement lacks audit event", count, err)
			}
		}
	}
	for generation, db := range index.admins {
		var count int
		if err = db.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM embeddings)+(SELECT count(*) FROM facts_projection_receipts)").Scan(&count); err != nil || count != 0 {
			t.Fatal("index copies retained", generation, count, err)
		}
	}
}
