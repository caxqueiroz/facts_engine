package workers

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

type refreshScheduler struct {
	versions []model.VersionReference
	requests []model.MemoryRefreshRequest
	failure  error
	failAt   int
}

func (s *refreshScheduler) ScanMemoryRefresh(_ context.Context, _ model.Caller, q model.MemoryRefreshScan) (model.MemoryRefreshScanResult, error) {
	out := model.MemoryRefreshScanResult{}
	for _, v := range s.versions {
		if v.VersionID > q.AfterVersionID {
			out.Versions = append(out.Versions, v)
			if len(out.Versions) == q.Limit {
				break
			}
		}
	}
	return out, nil
}
func (s *refreshScheduler) StartMemoryRefresh(_ context.Context, _ model.Caller, r model.MemoryRefreshRequest) (model.JobStatus, error) {
	s.requests = append(s.requests, r)
	if s.failAt == 0 || s.failAt == len(s.requests) {
		return model.JobStatus{}, s.failure
	}
	return model.JobStatus{State: "pending"}, nil
}
func TestMemoryRefreshSchedulingReplay(t *testing.T) {
	c := model.Caller{Tenant: "tenant", Principal: "scheduler", Service: true}
	scheduler := &refreshScheduler{failAt: 101, failure: model.ErrUnavailable}
	for i := 0; i < 105; i++ {
		scheduler.versions = append(scheduler.versions, model.VersionReference{RecordID: fmt.Sprintf("r%03d", i), VersionID: fmt.Sprintf("v%03d", i)})
	}
	worker, err := NewMemoryRefresh(scheduler, c, "v1", "dependency-review", []model.MemoryRefreshBinding{{Scope: "target", ProfileID: "profile"}})
	if err != nil {
		t.Fatal(err)
	}
	d := model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant, Consumer: model.MemoryRefreshConsumer("v1")}, Fence: 1, Event: model.Event{ID: "event", RecordID: "fact", Type: "supersede_fact", LedgerRevision: 50, Caller: c}}
	if err = worker.Handle(context.Background(), d); !errors.Is(err, model.ErrUnavailable) || len(scheduler.requests) != 101 {
		t.Fatal("partial scheduling failure swallowed", err, len(scheduler.requests))
	}
	keys := map[string]string{}
	for _, r := range scheduler.requests {
		keys[r.VersionID] = r.Key
	}
	scheduler.requests = nil
	scheduler.failure = nil
	scheduler.failAt = 0
	d.Fence++
	if err = worker.Handle(context.Background(), d); err != nil || len(scheduler.requests) != 105 {
		t.Fatal("paged replay", err, len(scheduler.requests))
	}
	for _, r := range scheduler.requests {
		if previous := keys[r.VersionID]; previous != "" && previous != r.Key {
			t.Fatal("delivery fence changed job identity")
		}
		if r.Trigger == nil || r.Trigger.EventID != "event" || r.Trigger.LedgerRevision != 50 || r.ProfileID != "profile" {
			t.Fatal("lost canonical trigger binding", r)
		}
	}
	scheduler.failure = model.ErrRefreshNotNeeded
	if err = worker.Handle(context.Background(), d); err != nil {
		t.Fatal("obsolete targets blocked scheduling", err)
	}
	for _, failure := range []error{model.ErrPrecondition, model.ErrDenied} {
		scheduler.failure = failure
		if err = worker.Handle(context.Background(), d); !errors.Is(err, failure) {
			t.Fatal("configuration/access failure skipped", err)
		}
	}
	d.Event.RecordKind = "memory"
	before := len(scheduler.requests)
	if err = worker.Handle(context.Background(), d); err != nil || len(scheduler.requests) != before {
		t.Fatal("memory change triggered fact dependency scan")
	}
	d.Key.Tenant = "other"
	if err = worker.Handle(context.Background(), d); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross-tenant delivery accepted", err)
	}
}
