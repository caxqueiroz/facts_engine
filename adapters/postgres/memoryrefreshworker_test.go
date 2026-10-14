package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/internal/workerdaemon"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log/slog"
	"testing"
	"time"
)

func testStandaloneRefresh(t *testing.T, ctx context.Context, dsn, endpoint, storeID, modelID, tenant string, fact model.CommitResult) {
	t.Helper()
	pool, err := pgxpool.New(ctx, databaseRole(t, dsn, "facts_runtime"))
	if err != nil {
		t.Fatalf("refresh fixture: %v", err)
	}
	defer pool.Close()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		t.Fatalf("refresh fixture: %v", err)
	}
	store, err := evidence.NewPostgres(pool, cipher)
	if err != nil {
		t.Fatalf("refresh fixture: %v", err)
	}
	engine, err := facts.New(ledger, allow{}, store)
	if err != nil {
		t.Fatalf("refresh fixture: %v", err)
	}
	caller := model.Caller{Tenant: tenant, Principal: "fixture-proposer"}
	reviewer := model.Caller{Tenant: tenant, Principal: "fixture-reviewer"}
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "refresh-review"} }
	if _, err := engine.RegisterMemoryType(ctx, caller, model.MemoryTypeRequest{Command: cmd("refresh-type"), Definition: model.MemoryType{Scope: "case1", Subtype: "summary", Version: "v1", PolicyID: "manual", Classification: "confidential"}}); err != nil {
		t.Fatalf("refresh fixture: %v", err)
	}
	from := time.Now().UTC().Truncate(time.Microsecond)
	memory, err := engine.ProposeMemory(ctx, caller, model.MemoryProposal{Command: cmd("refresh-memory"), Key: model.MemoryKey{Scope: "case1", ID: "summary"}, Subtype: "summary", SchemaVersion: "v1", Body: "Customer prefers tea.", Valid: model.Interval{From: &from}, Sources: []model.SourceSpan{{EvidenceID: "evidence", Start: 0, End: 3, Quote: "tea"}}, Dependencies: []model.MemoryDependency{{Scope: "case1", RecordID: fact.RecordID, VersionID: fact.VersionID, ContentDigest: fact.ContentDigest, Label: "assertion"}}})
	if err != nil {
		t.Fatalf("refresh fixture: %v", err)
	}
	if _, err := engine.ApproveMemory(ctx, reviewer, model.Approval{Command: cmd("refresh-approve"), Scope: "case1", RecordID: memory.RecordID, VersionID: memory.VersionID, ExpectedRevision: memory.StreamRevision}); err != nil {
		t.Fatalf("refresh fixture: %v", err)
	}
	changed, err := engine.DisputeFact(ctx, caller, model.LifecycleChange{Approval: model.Approval{Command: cmd("refresh-dispute"), Scope: "case1", RecordID: fact.RecordID, VersionID: fact.VersionID, ExpectedRevision: 2}, Valid: model.Interval{From: &from}, Reason: "review"})
	if err != nil {
		t.Fatalf("refresh fixture: %v", err)
	}
	prompt := []byte("Propose a replacement summary using supplied dependencies.")
	profile := model.ExtractionProfile{ID: "refresh-profile", ProviderID: "fixture", RequestedModel: "fixture-v1", PromptVersion: "v1", Prompt: prompt, PromptDigest: model.BytesDigest(prompt), ParserVersion: "memory-refresh-json-v1", Parameters: json.RawMessage(`{}`), ApplicationBuild: "test-v1", Timeout: time.Second, MaxInputBytes: 1 << 20, MaxOutputBytes: 1 << 20, MaxCandidates: 1, MaxAttempts: 2, MaxTokensPerAttempt: 1000, RetryDelay: time.Second}
	config := workerdaemon.Config{Mode: "refresh", Principal: "refresh-worker", Refresh: &workerdaemon.RefreshConfig{Tenant: tenant, Stream: "refresh-v1", Purpose: "refresh-review", Bindings: []workerdaemon.RefreshBinding{{Scope: "case1", ProfileID: profile.ID}}, Profiles: []model.ExtractionProfile{profile}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 10000}, OpenFGAURL: endpoint, OpenFGAStore: storeID, OpenFGAModel: modelID, MaxConnections: 4, MaxAttempts: 3, LeaseSeconds: 3, PollMilliseconds: 10, MinBackoffMilliseconds: 10, MaxBackoffSeconds: 1, DeliveryTimeoutSeconds: 5}
	t.Setenv("MEMORY_WORKER_DATABASE_URL", databaseRole(t, dsn, "facts_worker"))
	t.Setenv("MEMORY_DATABASE_URL", databaseRole(t, dsn, "facts_runtime"))
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- workerdaemon.Run(runCtx, config, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(6 * time.Second):
			t.Error("refresh worker did not stop")
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		changes, err := engine.ListChanges(ctx, caller, model.ChangesQuery{Scope: "case1", Purpose: "refresh-review", Limit: 1000})
		if err != nil {
			t.Fatalf("refresh fixture: %v", err)
		}
		for _, event := range changes.Events {
			if event.Type != "start_memory_refresh" {
				continue
			}
			var job model.ExtractionJob
			if err := ledger.Transact(ctx, tenant, func(tx model.Tx) error {
				var err error
				job, err = tx.(model.ExtractionTx).ExtractionJob(ctx, event.EntityID)
				return err
			}); err != nil {
				t.Fatalf("refresh fixture: %v", err)
			}
			if job.Caller.Principal != "refresh-worker" || !job.Caller.Service || job.ProfileDigest != model.ExtractionProfileDigest(profile) || job.Request.Refresh == nil || job.Request.Refresh.VersionID != memory.VersionID || job.Request.RefreshTrigger == nil || job.Request.RefreshTrigger.LedgerRevision != changed.LedgerRevision {
				t.Fatal("incorrect durable refresh job", job.ID)
			}
			status, err := engine.GetJob(ctx, caller, model.JobQuery{ID: job.ID, Scope: "case1", Purpose: "refresh-review"})
			if err != nil || status.State != "pending" || status.Attempts != 0 {
				t.Fatal("scheduler executed job", status, err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("standalone refresh did not schedule job")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
