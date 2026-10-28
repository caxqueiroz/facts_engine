package workers

import (
	"context"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

type cleanupScannerFunc func(context.Context, model.ProjectionCleanupQuery) ([]model.ProjectionCleanupRequest, error)

func (f cleanupScannerFunc) ScanProjectionCleanup(ctx context.Context, q model.ProjectionCleanupQuery) ([]model.ProjectionCleanupRequest, error) {
	return f(ctx, q)
}

type projectionCleanupEngineFunc func(context.Context, model.Caller, model.ProjectionCleanupRequest) (model.CommitResult, error)

func (f projectionCleanupEngineFunc) CleanupProjection(ctx context.Context, c model.Caller, r model.ProjectionCleanupRequest) (model.CommitResult, error) {
	return f(ctx, c, r)
}
func cleanupWorkerFixture() (ProjectionCleanupConfig, model.Caller, []model.ProjectionCleanupRequest) {
	config := ProjectionCleanupConfig{Tenant: "tenant", Stream: "cleanup", Reason: "expired", PageSize: 1, MaxPages: 1, IntervalMinutes: 1}
	caller := model.Caller{Tenant: "tenant", Principal: "worker", Service: true}
	requests := []model.ProjectionCleanupRequest{}
	for _, version := range []string{"a", "b"} {
		requests = append(requests, model.ProjectionCleanupRequest{Command: model.Command{Purpose: "retention"}, Kind: "fact", Scope: "scope", RecordID: "record", VersionID: version, Generation: "index", DisposalEventID: "disposal-" + version, Bindings: []model.VersionRetentionBinding{{Scope: "scope", PolicyID: "policy", Jurisdiction: "XX"}}})
	}
	return config, caller, requests
}
func TestProjectionCleanupFairnessAndRestart(t *testing.T) {
	config, caller, requests := cleanupWorkerFixture()
	scanner := cleanupScannerFunc(func(_ context.Context, q model.ProjectionCleanupQuery) ([]model.ProjectionCleanupRequest, error) {
		for _, r := range requests {
			if (model.ProjectionCleanupCursor{Kind: r.Kind, VersionID: r.VersionID, Generation: r.Generation}).Compare(q.After) > 0 {
				return []model.ProjectionCleanupRequest{r}, nil
			}
		}
		return nil, nil
	})
	keys := map[string][]string{}
	engine := projectionCleanupEngineFunc(func(_ context.Context, c model.Caller, r model.ProjectionCleanupRequest) (model.CommitResult, error) {
		if c != caller || r.Purpose != "retention" || r.Reason != config.Reason {
			t.Fatal(c, r)
		}
		keys[r.VersionID] = append(keys[r.VersionID], r.Key)
		if r.VersionID == "a" {
			return model.CommitResult{}, model.ErrPrecondition
		}
		return model.CommitResult{}, nil
	})
	worker, err := NewProjectionCleanup(scanner, engine, caller, config)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []ProjectionCleanupResult{{Scanned: 1, Skipped: 1}, {Scanned: 1, Completed: 1}, {}, {Scanned: 1, Skipped: 1}} {
		result, err := worker.Tick(context.Background())
		if err != nil || result != want {
			t.Fatal(i, result, err)
		}
	}
	restarted, _ := NewProjectionCleanup(scanner, engine, caller, config)
	if _, err = restarted.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(keys["a"]) != 3 || keys["a"][0] != keys["a"][1] || keys["a"][0] != keys["a"][2] {
		t.Fatal("restart changed command identity", keys)
	}
}
func TestProjectionCleanupErrorsAndMalformedPages(t *testing.T) {
	config, caller, requests := cleanupWorkerFixture()
	for _, failure := range []error{model.ErrDenied, model.ErrUnavailable, model.ErrConflict, context.Canceled, context.DeadlineExceeded} {
		calls := 0
		var key string
		scanner := cleanupScannerFunc(func(_ context.Context, q model.ProjectionCleanupQuery) ([]model.ProjectionCleanupRequest, error) {
			if q.After != (model.ProjectionCleanupCursor{}) {
				t.Fatal("failed target skipped")
			}
			return requests[:1], nil
		})
		engine := projectionCleanupEngineFunc(func(_ context.Context, _ model.Caller, r model.ProjectionCleanupRequest) (model.CommitResult, error) {
			calls++
			if key != "" && key != r.Key {
				t.Fatal("retry changed key")
			}
			key = r.Key
			return model.CommitResult{}, failure
		})
		worker, _ := NewProjectionCleanup(scanner, engine, caller, config)
		for range 2 {
			if _, err := worker.Tick(context.Background()); !errors.Is(err, failure) {
				t.Fatal(err)
			}
		}
		if calls != 2 {
			t.Fatal(calls)
		}
	}
	config.PageSize = 2
	for _, page := range [][]model.ProjectionCleanupRequest{{requests[1], requests[0]}, {requests[0], requests[0]}, {requests[0], requests[1], requests[1]}} {
		scanner := cleanupScannerFunc(func(context.Context, model.ProjectionCleanupQuery) ([]model.ProjectionCleanupRequest, error) {
			return page, nil
		})
		engine := projectionCleanupEngineFunc(func(context.Context, model.Caller, model.ProjectionCleanupRequest) (model.CommitResult, error) {
			t.Fatal("malformed page performed cleanup")
			return model.CommitResult{}, nil
		})
		worker, _ := NewProjectionCleanup(scanner, engine, caller, config)
		if _, err := worker.Tick(context.Background()); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	scanner := cleanupScannerFunc(func(context.Context, model.ProjectionCleanupQuery) ([]model.ProjectionCleanupRequest, error) {
		cancel()
		return nil, nil
	})
	engine := projectionCleanupEngineFunc(func(context.Context, model.Caller, model.ProjectionCleanupRequest) (model.CommitResult, error) {
		t.Fatal("canceled cleanup")
		return model.CommitResult{}, nil
	})
	worker, _ := NewProjectionCleanup(scanner, engine, caller, config)
	if _, err := worker.Tick(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
