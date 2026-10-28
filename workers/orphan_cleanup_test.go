package workers

import (
	"context"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

type cleanupEngine struct {
	scan    func(model.OrphanEvidenceQuery) ([]model.ObjectRef, error)
	dispose func(model.OrphanEvidenceDisposalRequest) (model.CommitResult, error)
}

func (e cleanupEngine) ScanOrphanEvidence(_ context.Context, _ model.Caller, q model.OrphanEvidenceQuery) ([]model.ObjectRef, error) {
	return e.scan(q)
}
func (e cleanupEngine) DisposeOrphanEvidence(_ context.Context, _ model.Caller, r model.OrphanEvidenceDisposalRequest) (model.CommitResult, error) {
	return e.dispose(r)
}
func TestOrphanCleanupBoundaries(t *testing.T) {
	config := OrphanCleanupConfig{Tenant: "org", Stream: "cleanup", Purpose: "cleanup", Reason: "expired", Bindings: []OrphanCleanupBinding{{Scope: "case", PolicyID: "policy", Jurisdiction: "XX"}}, PageSize: 1, MaxPages: 2, IntervalMinutes: 1}
	caller := model.Caller{Tenant: "org", Principal: "worker", Service: true}
	a := model.ObjectRef{Locator: "a", Version: model.BytesDigest([]byte("a")), Digest: model.BytesDigest([]byte("a")), Size: 1}
	b := a
	b.Locator = "b"
	for _, failure := range []error{model.ErrPrecondition, model.ErrDenied, model.ErrUnavailable, context.Canceled, context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			scanned, disposed := 0, 0
			engine := cleanupEngine{scan: func(q model.OrphanEvidenceQuery) ([]model.ObjectRef, error) {
				scanned++
				if q.AfterLocator == "" {
					return []model.ObjectRef{a}, nil
				}
				if q.AfterLocator != "a" || q.AfterVersion != a.Version {
					t.Fatal("cursor did not advance", q)
				}
				return []model.ObjectRef{b}, nil
			}, dispose: func(r model.OrphanEvidenceDisposalRequest) (model.CommitResult, error) {
				disposed++
				return model.CommitResult{}, failure
			}}
			worker, err := NewOrphanCleanup(engine, caller, config)
			if err != nil {
				t.Fatal(err)
			}
			result, err := worker.Tick(context.Background())
			if errors.Is(failure, model.ErrPrecondition) {
				if err != nil || result.Skipped != 2 || scanned != 2 || disposed != 2 {
					t.Fatal(result, err, scanned, disposed)
				}
			} else if !errors.Is(err, failure) || scanned != 1 || disposed != 1 {
				t.Fatal("failure swallowed", result, err, scanned, disposed)
			}
		})
	}
	var keys []string
	engine := cleanupEngine{scan: func(model.OrphanEvidenceQuery) ([]model.ObjectRef, error) { return []model.ObjectRef{a}, nil }, dispose: func(r model.OrphanEvidenceDisposalRequest) (model.CommitResult, error) {
		if r.Scope != "case" {
			t.Fatal("configuration mutation leaked", r.Scope)
		}
		keys = append(keys, r.Key)
		return model.CommitResult{}, nil
	}}
	config.MaxPages = 1
	worker, err := NewOrphanCleanup(engine, caller, config)
	if err != nil {
		t.Fatal(err)
	}
	config.Bindings[0].Scope = "mutated"
	for range 2 {
		if _, err := worker.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatal("retry keys changed", keys)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := worker.Tick(ctx); !errors.Is(err, context.Canceled) || len(keys) != 2 {
		t.Fatal("canceled cleanup performed work", err)
	}
}

func TestOrphanCleanupLateCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine := cleanupEngine{scan: func(model.OrphanEvidenceQuery) ([]model.ObjectRef, error) { cancel(); return nil, nil }, dispose: func(model.OrphanEvidenceDisposalRequest) (model.CommitResult, error) {
		t.Fatal("unexpected deletion")
		return model.CommitResult{}, nil
	}}
	config := OrphanCleanupConfig{Tenant: "org", Stream: "cleanup", Purpose: "cleanup", Reason: "expired", Bindings: []OrphanCleanupBinding{{Scope: "case", PolicyID: "policy", Jurisdiction: "XX"}}, PageSize: 1, MaxPages: 1, IntervalMinutes: 1}
	worker, err := NewOrphanCleanup(engine, model.Caller{Tenant: "org", Principal: "worker", Service: true}, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Tick(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("late cancellation swallowed", err)
	}
}
