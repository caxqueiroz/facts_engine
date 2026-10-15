package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/internal/workerdaemon"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testStandaloneCleanup(t *testing.T, ctx context.Context, dsn, endpoint, storeID, modelID, tenant string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, databaseRole(t, dsn, "facts_retention"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	store, err := evidence.NewPostgres(pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := facts.New(ledger, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	caller := model.Caller{Tenant: tenant, Principal: "cleanup-fixture"}
	policy := model.RetentionPolicy{ID: "daemon-orphans", Scope: "case1", RecordClass: "orphan_evidence_bytes", Purpose: "cleanup", Jurisdiction: "SG"}
	if _, err := engine.RegisterRetentionPolicy(ctx, caller, model.RetentionPolicyRequest{Command: model.Command{Key: "daemon-orphans", Purpose: "cleanup"}, Policy: policy}); err != nil {
		t.Fatal(err)
	}
	ref, err := store.PutPending(ctx, tenant, model.PendingEvidenceUpload{ID: "daemon-orphan", Scope: "case1", PolicyID: policy.ID}, []byte("pending"))
	if err != nil {
		t.Fatal(err)
	}
	config := workerdaemon.Config{Mode: "orphan-cleanup", Principal: "cleanup-worker", OpenFGAURL: endpoint, OpenFGAStore: storeID, OpenFGAModel: modelID, MaxConnections: 2, DeliveryTimeoutSeconds: 5, OrphanCleanup: &workers.OrphanCleanupConfig{Tenant: tenant, Stream: "cleanup", Purpose: "cleanup", Reason: "expired", Bindings: []workers.OrphanCleanupBinding{{Scope: "case1", PolicyID: policy.ID, Jurisdiction: "SG"}}, PageSize: 10, MaxPages: 2, IntervalMinutes: 1}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Setenv("MEMORY_WORKER_DATABASE_URL", databaseRole(t, dsn, "facts_runtime"))
	if err := workerdaemon.Run(ctx, config, logger); err == nil {
		t.Fatal("ordinary role ran cleanup")
	}
	if _, err := store.Read(ctx, tenant, ref); err != nil {
		t.Fatal("denied worker deleted object", err)
	}
	t.Setenv("MEMORY_WORKER_DATABASE_URL", databaseRole(t, dsn, "facts_retention"))
	denied := config
	denied.Principal = "unauthorized-cleanup"
	if err := workerdaemon.Run(ctx, denied, logger); err == nil {
		t.Fatal("unauthorized service ran cleanup")
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- workerdaemon.Run(runCtx, config, logger) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(6 * time.Second):
			t.Error("cleanup worker did not stop")
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := store.Read(ctx, tenant, ref)
		if errors.Is(err, model.ErrPrecondition) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("cleanup worker did not delete upload")
		}
		time.Sleep(10 * time.Millisecond)
	}
	changes, err := engine.ListChanges(ctx, caller, model.ChangesQuery{Scope: "case1", Purpose: "cleanup", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range changes.Events {
		if event.Type == "dispose_orphan_evidence" && event.Caller.Principal == "cleanup-worker" && event.Caller.Service {
			return
		}
	}
	t.Fatal("cleanup worker receipt identity missing")
}
