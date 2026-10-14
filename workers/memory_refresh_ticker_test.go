package workers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

type sweepStarter struct {
	requests  []model.MemoryRefreshSweepRequest
	failScope string
}

func (s *sweepStarter) StartMemoryRefreshSweep(_ context.Context, _ model.Caller, r model.MemoryRefreshSweepRequest) (model.CommitResult, error) {
	s.requests = append(s.requests, r)
	if r.Scope == s.failScope {
		return model.CommitResult{}, model.ErrUnavailable
	}
	return model.CommitResult{ID: r.Key}, nil
}
func TestMemoryRefreshTickIdentityAndCancellation(t *testing.T) {
	c := model.Caller{Tenant: "tenant", Principal: "ticker", Service: true}
	starter := &sweepStarter{failScope: "second"}
	bindings := []model.MemoryRefreshBinding{{Scope: "first", ProfileID: "profile"}, {Scope: "second", ProfileID: "profile"}}
	ticker, err := NewMemoryRefreshTicker(starter, c, "v1", "expiry-review", bindings, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bindings[0].Scope = "mutated"
	at := time.Date(2026, 1, 1, 12, 0, 20, 0, time.UTC)
	partial, err := ticker.Tick(context.Background(), at)
	if !errors.Is(err, model.ErrUnavailable) || len(partial) != 1 || starter.requests[0].Scope != "first" {
		t.Fatal(partial, err)
	}
	key := starter.requests[0].Key
	starter.failScope = ""
	again, err := ticker.Tick(context.Background(), at.Add(10*time.Second))
	if err != nil || len(again) != 2 || again[0].ID != key {
		t.Fatal("same interval lost retry identity", again, err)
	}
	later, err := ticker.Tick(context.Background(), at.Add(time.Minute))
	if err != nil || later[0].ID == key {
		t.Fatal("next interval reused old tick", later, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := len(starter.requests)
	if err = ticker.Run(ctx); !errors.Is(err, context.Canceled) || len(starter.requests) != before {
		t.Fatal("canceled ticker created events", err)
	}
	if _, err = NewMemoryRefreshTicker(starter, c, "v1", "review", bindings, time.Second); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("unbounded tick frequency", err)
	}
}

func TestMemoryRefreshSweepRouting(t *testing.T) {
	c := model.Caller{Tenant: "tenant", Principal: "scheduler", Service: true}
	scheduler := &refreshScheduler{versions: []model.VersionReference{{RecordID: "record", VersionID: "version"}}}
	worker, err := NewMemoryRefresh(scheduler, c, "v1", "review", []model.MemoryRefreshBinding{{Scope: "scope", ProfileID: "profile"}})
	if err != nil {
		t.Fatal(err)
	}
	d := model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant, Consumer: model.MemoryRefreshConsumer("v1")}, Fence: 1, Event: model.Event{ID: "sweep", Type: "sweep_memory_refresh", Scope: "scope", Caller: c, LedgerRevision: 10, Command: model.Command{Purpose: "review"}, RefreshSweep: &model.MemoryRefreshSweep{Stream: "other", ProfileID: "profile"}}}
	if err = worker.Handle(context.Background(), d); err != nil || len(scheduler.requests) != 0 {
		t.Fatal("foreign stream tick processed", err)
	}
	d.Event.RefreshSweep.Stream = "v1"
	if err = worker.Handle(context.Background(), d); err != nil || len(scheduler.requests) != 1 {
		t.Fatal("sweep not scheduled", err)
	}
	d.Event.RefreshSweep.ProfileID = "changed"
	if err = worker.Handle(context.Background(), d); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("sweep profile mismatch ignored", err)
	}
}
