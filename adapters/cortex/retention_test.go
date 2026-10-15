package cortex_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
)

type heldRetentionEmbedder struct{ started, release chan struct{} }

func (e heldRetentionEmbedder) Embed(ctx context.Context, _ string) ([]float32, error) {
	close(e.started)
	select {
	case <-e.release:
		return []float32{1, 0, 0}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func testProjectionDisposal(t *testing.T, ctx context.Context, writer *cortex.Index, admin *sql.DB, dsn string, doc model.ProjectionDocument) {
	t.Helper()
	config := cortex.Config{Tenant: doc.Tenant, Generation: doc.Generation, EmbeddingID: "test-constant-v1", Dimensions: 3}
	// Reproduce a pre-retention index without changing its content or receipts.
	if _, err := admin.ExecContext(ctx, `DROP TRIGGER facts_guard_disposed_projection ON embeddings; DROP FUNCTION facts_guard_disposed_projection(); DROP TABLE facts_projection_tombstones`); err != nil {
		t.Fatal(err)
	}
	runtime, err := sql.Open("pgx", dsn+"&options=-c%20role%3Dcortex_test_runtime")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	runtime.SetMaxOpenConns(4)
	if _, err := cortex.New(ctx, runtime, config, embedder{}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("legacy schema accepted", err)
	}
	if err := cortex.ProvisionRetention(ctx, admin); err != nil {
		t.Fatal(err)
	}
	if err := cortex.ProvisionRetention(ctx, admin); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("retention schema re-provisioned", err)
	}
	grants, err := os.ReadFile("../../scripts/cortex-runtime-role.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, string(grants)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, `GRANT SELECT ON facts_projection_tombstones TO cortex_test_runtime`); err != nil {
		t.Fatal(err)
	}
	retentionDB, err := sql.Open("pgx", dsn+"&options=-c%20role%3Dfacts_cortex_retention")
	if err != nil {
		t.Fatal(err)
	}
	defer retentionDB.Close()
	retentionDB.SetMaxOpenConns(4)
	retention, err := cortex.New(ctx, retentionDB, config, embedder{})
	if err != nil {
		t.Fatal(err)
	}
	target := model.ProjectionDisposal{Tenant: doc.Tenant, Generation: doc.Generation, Kind: "fact", VersionID: doc.VersionID}
	if err := writer.Put(ctx, doc); err != nil {
		t.Fatal("upgrade changed receipt", err)
	}
	if err := writer.DisposeProjection(ctx, target); err == nil {
		t.Fatal("ordinary writer disposed content")
	} else {
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Fatal(err)
		}
	}
	memory := doc
	memory.Key = model.FactKey{}
	memory.Value = model.Value{}
	memory.Memory = &model.MemoryProjection{Key: model.MemoryKey{Scope: doc.Scope(), ID: "summary"}, Subtype: "episode", SchemaVersion: "v1", Body: "canonical memory"}
	if err := writer.Put(ctx, memory); err != nil {
		t.Fatal(err)
	}
	wrong := target
	wrong.Tenant = "other"
	if err := retention.DisposeProjection(ctx, wrong); !errors.Is(err, model.ErrDenied) {
		t.Fatal(err)
	}
	wrong = target
	wrong.Generation = "other"
	if err := retention.DisposeProjection(ctx, wrong); !errors.Is(err, model.ErrDenied) {
		t.Fatal(err)
	}
	wrong = target
	wrong.Kind = "unknown"
	if err := retention.DisposeProjection(ctx, wrong); !errors.Is(err, model.ErrInvalid) {
		t.Fatal(err)
	}
	// A failure after row deletion must roll back the row, receipt and tombstone.
	if _, err := admin.ExecContext(ctx, `REVOKE DELETE ON facts_projection_receipts FROM facts_cortex_retention`); err != nil {
		t.Fatal(err)
	}
	if err := retention.DisposeProjection(ctx, target); err == nil {
		t.Fatal("injected privilege failure ignored")
	}
	if _, err := admin.ExecContext(ctx, `GRANT DELETE ON facts_projection_receipts TO facts_cortex_retention`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM facts_projection_tombstones`).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed disposal left tombstone", count, err)
	}
	if err := writer.Put(ctx, doc); err != nil {
		t.Fatal("rollback lost receipt", err)
	}
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM embeddings WHERE id=$1`, model.ProjectionID(doc)).Scan(&count); err != nil || count != 1 {
		t.Fatal("rollback lost content", count, err)
	}
	routed, _ := cortex.NewGenerations(retention)
	if err := routed.DisposeProjection(ctx, target); err != nil {
		t.Fatal(err)
	}
	for _, d := range []model.ProjectionDocument{doc, memory} {
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM embeddings WHERE id=$1`, model.ProjectionID(d)).Scan(&count); err != nil {
			t.Fatal(err)
		}
		want := 0
		if d.Memory != nil {
			want = 1
		}
		if count != want {
			t.Fatal("kind isolation", count, want)
		}
	}
	if err := writer.Put(ctx, doc); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("late write resurrected fact", err)
	}
	// The database trigger also protects writes from older adapters.
	_, err = admin.ExecContext(ctx, `INSERT INTO embeddings(id,collection_id,vector,content,metadata) SELECT $1,collection_id,vector,content,metadata FROM embeddings WHERE id=$2`, model.ProjectionID(doc), model.ProjectionID(memory))
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "23514" {
		t.Fatal("direct write bypassed tombstone", err)
	}
	target.Kind = "memory"
	if err := retention.Lexical().DisposeProjection(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := writer.Put(ctx, memory); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("memory resurrected", err)
	}
	for _, view := range []model.RetrievalIndex{writer, writer.Lexical()} {
		hits, err := view.Candidates(ctx, doc.Tenant, doc.Generation, doc.Scope(), "canonical", 10)
		if err != nil || len(hits) != 0 {
			t.Fatal("disposed content searchable", hits, err)
		}
	}
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM facts_projection_receipts`).Scan(&count); err != nil || count != 0 {
		t.Fatal("disposed receipt retained", count, err)
	}
	key := model.GenerationKey{Tenant: doc.Tenant, Generation: doc.Generation}
	if err := writer.SetProjectionGate(ctx, key, 1, true); err != nil {
		t.Fatal(err)
	}
	if err := retention.DisposeProjection(ctx, target); err != nil {
		t.Fatal("sealed exact retry failed", err)
	}
	fresh := target
	fresh.Kind = "fact"
	fresh.VersionID = "not-yet-written"
	if err := retention.DisposeProjection(ctx, fresh); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("sealed generation mutated", err)
	}
	if err := writer.SetProjectionGate(ctx, key, 2, false); err != nil {
		t.Fatal(err)
	}
	held := heldRetentionEmbedder{make(chan struct{}), make(chan struct{})}
	delayed, err := cortex.New(ctx, runtime, config, held)
	if err != nil {
		t.Fatal(err)
	}
	late := doc
	late.VersionID = fresh.VersionID
	done := make(chan error, 1)
	go func() { done <- delayed.Put(ctx, late) }()
	<-held.started
	if err := (cortex.Router{doc.Tenant: retention}).DisposeProjection(ctx, fresh); err != nil {
		close(held.release)
		t.Fatal(err)
	}
	close(held.release)
	if err := <-done; !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("delayed embedding resurrected content", err)
	}
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM embeddings`).Scan(&count); err != nil || count != 0 {
		t.Fatal("late content retained", count, err)
	}
}
