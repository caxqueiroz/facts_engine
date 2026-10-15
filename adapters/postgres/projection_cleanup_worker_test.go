package postgres_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/adapters/openfga"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/authz"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/internal/workerdaemon"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
)

func testStandaloneProjectionCleanup(t *testing.T, ctx context.Context, ledger *postgres.Ledger, tenant, version string) {
	t.Helper()
	endpoint := os.Getenv("TEST_OPENFGA_URL")
	if endpoint == "" {
		t.Fatal("cleanup fixture requires OpenFGA")
	}
	httpClient := &http.Client{Timeout: 5 * time.Second}
	post := func(path string, body any) map[string]json.RawMessage {
		t.Helper()
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+path, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := httpClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			t.Fatalf("OpenFGA provisioning status %d", response.StatusCode)
		}
		out := map[string]json.RawMessage{}
		b, err = io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > 0 {
			if err := json.Unmarshal(b, &out); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	str := func(value json.RawMessage) string {
		var out string
		if err := json.Unmarshal(value, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	store := str(post("/stores", map[string]string{"name": "projection-cleanup-integration"})["id"])
	modelID := str(post("/stores/"+store+"/authorization-models", json.RawMessage(authz.ModelJSON))["authorization_model_id"])

	principal := "projection-cleanup-worker"
	service := "service:" + openfga.ObjectID(tenant, principal)
	tenantObject := "tenant:" + tenant
	tuples := []map[string]string{{"user": service, "relation": "member", "object": tenantObject}}
	for _, scope := range []string{"a", "z"} {
		object := "scope:" + openfga.ObjectID(tenant, scope)
		tuples = append(tuples, map[string]string{"user": tenantObject, "relation": "tenant", "object": object}, map[string]string{"user": service, "relation": "disposal_operator", "object": object})
	}
	post("/stores/"+store+"/write", map[string]any{"authorization_model_id": modelID, "writes": map[string]any{"tuple_keys": tuples}})
	t.Setenv("MEMORY_WORKER_DATABASE_URL", databaseRole(t, os.Getenv("TEST_DATABASE_URL"), "facts_retention"))
	t.Setenv("MEMORY_ENCRYPTION_KEY_HEX", hex.EncodeToString(bytes.Repeat([]byte{4}, 32)))
	t.Setenv("MEMORY_CLEANUP_STATIC_URL", databaseRole(t, os.Getenv("TEST_CLEANUP_DATABASE_URL"), "facts_cortex_retention"))
	t.Setenv("MEMORY_CLEANUP_TERMINAL_URL", databaseRole(t, os.Getenv("TEST_CLEANUP_TERMINAL_DATABASE_URL"), "facts_cortex_retention"))
	config := workerdaemon.Config{Mode: "projection-cleanup", Principal: principal, OpenFGAURL: endpoint, OpenFGAStore: store, OpenFGAModel: modelID, MaxConnections: 4, DeliveryTimeoutSeconds: 10, ProjectionCleanup: &workerdaemon.ProjectionCleanupConfig{ProjectionCleanupConfig: workers.ProjectionCleanupConfig{Tenant: tenant, Stream: "cleanup-worker", Reason: "expired", PageSize: 1, MaxPages: 4, IntervalMinutes: 1}}}
	for _, r := range []struct{ generation, env string }{{"static", "MEMORY_CLEANUP_STATIC_URL"}, {"terminal", "MEMORY_CLEANUP_TERMINAL_URL"}} {
		config.ProjectionCleanup.Destinations = append(config.ProjectionCleanup.Destinations, runtimeconfig.CleanupRoute{LexicalRoute: runtimeconfig.LexicalRoute{Tenant: tenant, Generation: r.generation, DatabaseEnv: r.env, MaxConnections: 4}, EmbeddingID: "test-constant-v1", Dimensions: 3})
	}
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
			t.Error("cleanup worker did not stop")
		}
	}()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		status, err := ledger.ProjectionCleanupStatus(ctx, model.ProjectionCleanupKey{Tenant: tenant, Kind: "memory", VersionID: version})
		if err != nil {
			t.Fatal(err)
		}
		if status.Complete {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("cleanup worker did not acknowledge target")
		case <-poll.C:
		}
	}
	pending, err := ledger.ScanProjectionCleanup(ctx, model.ProjectionCleanupQuery{Tenant: tenant, Limit: 100})
	if err != nil || len(pending) != 0 {
		t.Fatal("completed targets remain pending", pending, err)
	}
}
