package postgres_test

import (
	"bytes"
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"context"
	"database/sql"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/internal/workerdaemon"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

func databaseRole(t *testing.T, dsn, role string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		t.Fatal("integration requires PostgreSQL URL")
	}
	q := u.Query()
	q.Set("role", role)
	u.RawQuery = q.Encode()
	return u.String()
}
func standaloneRoute(t *testing.T, ctx context.Context, tenant string) runtimeconfig.LexicalRoute {
	t.Helper()
	dsn := os.Getenv("TEST_STANDALONE_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_STANDALONE_DATABASE_URL")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	if err := cortex.Provision(ctx, db, cortex.Config{Tenant: tenant, Generation: "standalone-g1", EmbeddingID: "none", Dimensions: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='standalone_index_runtime') THEN CREATE ROLE standalone_index_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$; GRANT USAGE ON SCHEMA public TO standalone_index_runtime; GRANT SELECT ON collections,facts_projection_identity TO standalone_index_runtime; GRANT SELECT,INSERT,UPDATE ON embeddings TO standalone_index_runtime; GRANT SELECT,UPDATE ON facts_projection_gate TO standalone_index_runtime; GRANT SELECT,INSERT ON facts_projection_receipts TO standalone_index_runtime`); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MEMORY_TEST_INDEX_URL", databaseRole(t, dsn, "standalone_index_runtime"))
	return runtimeconfig.LexicalRoute{Tenant: tenant, Generation: "standalone-g1", DatabaseEnv: "MEMORY_TEST_INDEX_URL", MaxConnections: 4}
}
func testStandaloneProjection(t *testing.T, ctx context.Context, client *http.Client, base, dsn, endpoint, storeID, modelID, principal string, route runtimeconfig.LexicalRoute) model.CommitResult {
	t.Helper()
	pool, err := pgxpool.New(ctx, databaseRole(t, dsn, "facts_runtime"))
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
	caller := model.Caller{Tenant: route.Tenant, Principal: "fixture-proposer"}
	reviewer := model.Caller{Tenant: route.Tenant, Principal: "fixture-reviewer"}
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "integration"} }
	if _, err := engine.PutSubject(ctx, caller, model.SubjectRequest{Command: cmd("subject"), Definition: model.SubjectDefinition{Scope: "case1", ID: "subject", Kind: "customer", Label: "Fixture"}, Reason: "Fixture"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RegisterAcceptancePolicy(ctx, caller, model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{Scope: "case1", ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RegisterPredicateVersion(ctx, caller, model.PredicateRequest{Command: cmd("predicate"), Definition: model.PredicateVersion{Scope: "case1", Predicate: "preference", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(ctx, route.Tenant, "source", []byte("tea"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RegisterEvidence(ctx, caller, model.EvidenceRequest{Command: cmd("evidence"), Evidence: model.Evidence{ID: "evidence", Scope: "case1", SourceSystem: "chat", SourceObject: "message", SourceRevision: "v1", Speaker: "customer", SourceClass: "customer", SourceTime: time.Now(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "test", Object: ref}}); err != nil {
		t.Fatal(err)
	}
	value := "tea"
	validAt := time.Now().UTC().Truncate(time.Microsecond)
	proposed, err := engine.ProposeFact(ctx, caller, model.FactProposal{Command: cmd("propose"), Key: model.FactKey{Scope: "case1", Subject: "subject", Predicate: "preference"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &validAt}, Sources: []model.SourceSpan{{EvidenceID: "evidence", Start: 0, End: 3, Quote: "tea"}}})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := engine.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("approve"), Scope: "case1", RecordID: proposed.RecordID, VersionID: proposed.VersionID, ExpectedRevision: proposed.StreamRevision})
	if err != nil {
		t.Fatal(err)
	}
	rpc := connect.NewClient(connecthttp.NewTransport(client, base))
	operations := memoryv1connect.NewOperationsServiceClient(rpc)
	if _, err := operations.RebuildIndex(ctx, &v1.RebuildIndexRequest{Tenant: route.Tenant, Generation: route.Generation, Purpose: "index-setup", Action: "start", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MEMORY_WORKER_DATABASE_URL", databaseRole(t, dsn, "facts_projector"))
	workerConfig := workerdaemon.Config{Mode: "projection", Principal: principal, Projection: route, OpenFGAURL: endpoint, OpenFGAStore: storeID, OpenFGAModel: modelID, MaxConnections: 4, MaxAttempts: 3, LeaseSeconds: 3, PollMilliseconds: 10, MinBackoffMilliseconds: 10, MaxBackoffSeconds: 1, DeliveryTimeoutSeconds: 5}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- workerdaemon.Run(runCtx, workerConfig, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(6 * time.Second):
			t.Error("projection worker did not stop")
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, err := operations.GetProjectionStatus(ctx, &v1.GetProjectionStatusRequest{Tenant: route.Tenant, Generation: route.Generation, Purpose: "verify-progress"})
		if err != nil {
			t.Fatal(err)
		}
		if status.Consumer.AcknowledgedRevision >= accepted.LedgerRevision {
			break
		}
		if status.Consumer.State == "failed" || time.Now().After(deadline) {
			t.Fatalf("worker did not project: %v", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	verified, err := operations.RebuildIndex(ctx, &v1.RebuildIndexRequest{Tenant: route.Tenant, Generation: route.Generation, Purpose: "verify", Action: "verify"})
	if err != nil || verified.Status.Count != 1 {
		t.Fatalf("verify projection: %v, %v", verified, err)
	}
	if _, err := operations.RebuildIndex(ctx, &v1.RebuildIndexRequest{Tenant: route.Tenant, Generation: route.Generation, Purpose: "activate", Action: "activate", ExpectedFence: verified.Status.Fence}); err != nil {
		t.Fatal(err)
	}
	retrieval := memoryv1connect.NewRetrievalServiceClient(rpc)
	result, err := retrieval.Recall(ctx, &v1.RecallRequest{Tenant: route.Tenant, Query: &v1.RecallQuery{Scopes: []string{"case1"}, Query: "tea", Purpose: "review", ValidAt: timestamppb.New(validAt), RequireGeneration: route.Generation, RequireRevision: accepted.LedgerRevision, Limit: 10}})
	if err != nil || result.Incomplete || len(result.Items) != 1 || result.Items[0].Fact.VersionId != proposed.VersionID || result.Items[0].Fact.Value.GetText() != "tea" {
		t.Fatalf("standalone lexical recall: %v, %v", result, err)
	}
	return proposed
}
