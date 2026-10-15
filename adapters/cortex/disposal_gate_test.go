package cortex_test

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
)

func testDisposalGate(t *testing.T, ctx context.Context, writer, retention *cortex.Index, admin *sql.DB, doc model.ProjectionDocument) {
	t.Helper()
	key := model.GenerationKey{Tenant: doc.Tenant, Generation: doc.Generation}
	doc.VersionID = "terminal-copy"
	if err := writer.Put(ctx, doc); err != nil {
		t.Fatal(err)
	}
	// Upgrade an existing destination without rewriting receipts or tombstones.
	if _, err := admin.ExecContext(ctx, `DROP TRIGGER facts_guard_disposal_gate ON facts_projection_gate; DROP FUNCTION facts_guard_disposal_gate(); ALTER TABLE facts_projection_gate DROP CONSTRAINT facts_disposal_gate_state; ALTER TABLE facts_projection_gate DROP COLUMN disposal_fence`); err != nil {
		t.Fatal(err)
	}
	cfg := cortex.Config{Tenant: doc.Tenant, Generation: doc.Generation, EmbeddingID: "test-constant-v1", Dimensions: 3}
	if _, err := cortex.New(ctx, admin, cfg, embedder{}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("legacy disposal gate accepted", err)
	}
	if err := cortex.ProvisionDisposalGate(ctx, admin); err != nil {
		t.Fatal(err)
	}
	if err := cortex.ProvisionDisposalGate(ctx, admin); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal(err)
	}
	grants, err := os.ReadFile("../../scripts/cortex-runtime-role.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.ExecContext(ctx, string(grants)); err != nil {
		t.Fatal(err)
	}
	if err = writer.Put(ctx, doc); err != nil {
		t.Fatal("upgrade changed receipt", err)
	}
	for _, fence := range []int64{0, math.MaxInt64} {
		if err = retention.SealProjectionForDisposal(ctx, key, fence); !errors.Is(err, model.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if err = retention.SealProjectionForDisposal(ctx, model.GenerationKey{Tenant: "other", Generation: key.Generation}, 3); !errors.Is(err, model.ErrDenied) {
		t.Fatal(err)
	}
	if err = retention.SealProjectionForDisposal(ctx, key, 2); !errors.Is(err, model.ErrRevision) {
		t.Fatal("unsealed generation retired", err)
	}
	if err = writer.SetProjectionGate(ctx, key, 3, true); err != nil {
		t.Fatal(err)
	}
	if _, err = writer.VerifyProjection(ctx, key, 3, []model.ProjectionDocument{doc}); err != nil {
		t.Fatal("fixture proof", err)
	}
	if err = writer.SealProjectionForDisposal(ctx, key, 3); err == nil {
		t.Fatal("writer entered disposal state")
	} else {
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Fatal(err)
		}
	}
	if err = retention.SealProjectionForDisposal(ctx, key, 2); !errors.Is(err, model.ErrRevision) {
		t.Fatal("stale fence", err)
	}
	// A verifier's shared gate lock prevents terminal transition beneath its proof.
	proofTx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer proofTx.Rollback()
	var proofFence int64
	if err = proofTx.QueryRowContext(ctx, `SELECT fence FROM facts_projection_gate WHERE singleton FOR SHARE`).Scan(&proofFence); err != nil || proofFence != 3 {
		t.Fatal(proofFence, err)
	}
	blocked, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	err = retention.SealProjectionForDisposal(blocked, key, 3)
	cancel()
	if err == nil {
		t.Fatal("disposal invalidated an in-flight proof")
	}
	if err = proofTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err = writer.VerifyProjection(ctx, key, 3, []model.ProjectionDocument{doc}); err != nil {
		t.Fatal("failed transition changed the proof", err)
	}
	routed, _ := cortex.NewGenerations(retention)
	if err = routed.SealProjectionForDisposal(ctx, key, 3); err != nil {
		t.Fatal(err)
	}
	if err = retention.Lexical().SealProjectionForDisposal(ctx, key, 3); err != nil {
		t.Fatal("exact retry", err)
	}
	if err = (cortex.Router{key.Tenant: retention}).SealProjectionForDisposal(ctx, key, 4); !errors.Is(err, model.ErrRevision) {
		t.Fatal("changed retry", err)
	}
	for _, fence := range []int64{3, 4} {
		if _, err = writer.VerifyProjection(ctx, key, fence, []model.ProjectionDocument{doc}); !errors.Is(err, model.ErrRevision) {
			t.Fatal("terminal proof accepted", err)
		}
		if _, err = writer.VerifyProjectionPages(ctx, key, fence, func(context.Context, model.ProjectionCursor) (model.ProjectionPage, error) {
			t.Fatal("terminal source read")
			return model.ProjectionPage{}, nil
		}); !errors.Is(err, model.ErrRevision) {
			t.Fatal("terminal paged proof accepted", err)
		}
	}
	// A delayed retirement/cutover retry may confirm its original sealed gate.
	// It must neither undo the terminal transition nor fail a committed cutover.
	if err = writer.SetProjectionGate(ctx, key, 3, true); err != nil {
		t.Fatal("retirement retry failed", err)
	}
	if err = writer.SetProjectionGate(ctx, key, 4, true); !errors.Is(err, model.ErrRevision) {
		t.Fatal("terminal gate refenced", err)
	}
	if err = writer.SetProjectionGate(ctx, key, 5, false); !errors.Is(err, model.ErrRevision) {
		t.Fatal("terminal destination reopened", err)
	}
	for _, statement := range []string{`UPDATE facts_projection_gate SET disposal_fence=NULL`, `UPDATE facts_projection_gate SET sealed=false,disposal_fence=NULL,fence=5`} {
		_, err = admin.ExecContext(ctx, statement)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "23514" {
			t.Fatal("direct update undid permanent gate", err)
		}
	}
	target := model.ProjectionDisposal{Tenant: key.Tenant, Generation: key.Generation, Kind: "fact", VersionID: doc.VersionID}
	if err = retention.DisposeProjection(ctx, target); err != nil {
		t.Fatal("sealed terminal cleanup", err)
	}
	if err = retention.DisposeProjection(ctx, target); err != nil {
		t.Fatal("cleanup retry", err)
	}
	if err = writer.Put(ctx, doc); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("terminal content resurrected", err)
	}
	var count int
	if err = admin.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM embeddings)+(SELECT count(*) FROM facts_projection_receipts)`).Scan(&count); err != nil || count != 0 {
		t.Fatal("terminal content retained", count, err)
	}
}
