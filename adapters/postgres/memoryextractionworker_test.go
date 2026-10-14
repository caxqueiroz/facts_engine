package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/internal/workerdaemon"
	"github.com/caxqueiroz/facts_engine/model"
)

func testStandaloneExtraction(t *testing.T, ctx context.Context, config workerdaemon.Config, engine *facts.Engine, caller model.Caller, jobID, dsn string) {
	t.Helper()
	var mu sync.Mutex
	requests := map[string][]byte{}
	responses := map[string][]byte{}
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
		if err != nil || len(raw) > 1<<20 || r.Header.Get("Authorization") != "Bearer fixture-secret" {
			http.Error(w, "invalid", 400)
			return
		}
		var input model.ExtractionInput
		if json.Unmarshal(raw, &input) != nil || input.Refresh == nil || len(input.Refresh.Dependencies) != 1 || len(input.Refresh.Dependencies[0].Versions) == 0 {
			http.Error(w, "invalid", 400)
			return
		}
		source := input.Refresh.Dependencies[0]
		version := source.Versions[0]
		body, _ := json.Marshal(model.MemoryRefreshOutput{Body: "Customer preference is disputed.", Dependencies: []model.MemoryDependency{{Scope: source.Scope, RecordID: source.RecordID, VersionID: version.ID, ContentDigest: version.Digest, Label: "disputed"}}})
		mu.Lock()
		requests[model.BytesDigest(raw)] = raw
		responses[model.BytesDigest(raw)] = body
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Facts-Model-Identity", "fixture-v1")
		w.Header().Set("X-Facts-Input-Tokens", "7")
		w.Header().Set("X-Facts-Output-Tokens", "3")
		_, _ = w.Write(body)
	}))
	defer gateway.Close()
	caFile := filepath.Join(t.TempDir(), "gateway-ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: gateway.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MEMORY_TEST_GATEWAY_TOKEN", "fixture-secret")
	t.Setenv("MEMORY_WORKER_DATABASE_URL", databaseRole(t, dsn, "facts_worker"))
	refresh := config.Refresh
	config.Mode = "extraction"
	config.Refresh = nil
	config.Extraction = &workerdaemon.ExtractionConfig{Tenant: caller.Tenant, Stream: "execution-v1", Scheduling: &runtimeconfig.ExtractionScheduling{Profiles: refresh.Profiles, MaxConcurrentPerTenant: 1, DailyTokenBudget: 10000}, Gateways: []workerdaemon.ExtractionGateway{{ProfileID: refresh.Profiles[0].ID, URL: gateway.URL, TokenEnv: "MEMORY_TEST_GATEWAY_TOKEN", RootCAFile: caFile}}}
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
			t.Error("extraction worker did not stop")
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, err := engine.GetJob(ctx, caller, model.JobQuery{ID: jobID, Scope: "case1", Purpose: "refresh-review"})
		if err != nil {
			t.Fatal(err)
		}
		if status.State == "completed" {
			run, err := engine.GetExtractionRun(ctx, caller, model.ExtractionRunQuery{ID: status.AttemptID, Scope: "case1", Purpose: "refresh-review"})
			if err != nil || run.Result == nil || len(run.Result.Proposals) != 1 || run.Result.ModelIdentity != "fixture-v1" || run.Result.InputTokens != 7 || run.Result.OutputTokens != 3 {
				t.Fatal("executor did not retain proposal and metadata", run.Result, err)
			}
			mu.Lock()
			request := append([]byte(nil), requests[run.Result.RequestDigest]...)
			response := append([]byte(nil), responses[run.Result.RequestDigest]...)
			mu.Unlock()
			if !bytes.Equal(request, run.Attempt.RequestBytes) || !bytes.Equal(response, run.Result.ResponseBytes) {
				t.Fatal("gateway boundary bytes changed")
			}
			current, err := engine.GetMemory(ctx, caller, model.MemoryQuery{Key: model.MemoryKey{Scope: "case1", ID: "summary"}, ValidAt: time.Now(), Purpose: "refresh-review"})
			if err != nil || current.State != "stale" || current.Body != "" {
				t.Fatal("executor auto-accepted proposal", current, err)
			}
			return
		}
		if status.State == "failed" || time.Now().After(deadline) {
			t.Fatal("executor failed", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
