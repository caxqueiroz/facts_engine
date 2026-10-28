package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
)

func testAutomaticMemoryRefresh(t *testing.T, ctx context.Context, engine *facts.Engine, l model.Ledger, store *evidence.Postgres, queue model.Outbox, c model.Caller, r model.MemoryRefreshRequest, original model.CommitResult) model.JobStatus {
	t.Helper()
	handler, err := workers.NewMemoryRefresh(engine, c, "dependency-v1", r.Purpose, []model.MemoryRefreshBinding{{Scope: r.Scope, ProfileID: r.ProfileID}})
	if err != nil {
		t.Fatal(err)
	}
	key := model.ConsumerKey{Tenant: c.Tenant, Consumer: model.MemoryRefreshConsumer("dependency-v1")}
	if err = queue.RegisterConsumer(ctx, key, 3); err != nil {
		t.Fatal(err)
	}
	worker, err := workers.NewOutbox(queue, key, handler, workers.Options{Lease: time.Minute, PollInterval: time.Millisecond, MinBackoff: time.Second, MaxBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; ; i++ {
		if i > 1000 {
			t.Fatal("unbounded refresh scheduling")
		}
		worked, err := worker.ProcessOne(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			status, err := queue.ConsumerStatus(ctx, key)
			if err != nil || status.State != "pending" || status.LastFailureCode != "" {
				t.Fatal("refresh consumer failed", status, err)
			}
			break
		}
	}
	changes, err := engine.ListChanges(ctx, c, model.ChangesQuery{Scope: r.Scope, Purpose: r.Purpose, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var trigger model.Event
	var scheduled model.ExtractionJob
	count := 0
	for _, event := range changes.Events {
		if event.Type == "supersede_fact" {
			trigger = event
		}
		if event.Type != "start_memory_refresh" {
			continue
		}
		var job model.ExtractionJob
		err = l.Transact(ctx, c.Tenant, func(tx model.Tx) error {
			var err error
			job, err = tx.(model.ExtractionTx).ExtractionJob(ctx, event.EntityID)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if job.Request.RefreshTrigger != nil {
			scheduled = job
			count++
		}
	}
	if count != 1 || trigger.ID == "" || scheduled.Request.RefreshTrigger.EventID != trigger.ID || scheduled.Request.RefreshTrigger.LedgerRevision != trigger.LedgerRevision || scheduled.Request.Refresh.VersionID != original.VersionID {
		t.Fatal("missing canonical refresh trigger", scheduled, trigger, count)
	}
	scan := model.MemoryRefreshScan{Scope: r.Scope, Purpose: r.Purpose, Trigger: *scheduled.Request.RefreshTrigger, Limit: 1}
	page, err := engine.ScanMemoryRefresh(ctx, c, scan)
	if err != nil || len(page.Versions) != 1 || page.Versions[0].VersionID != original.VersionID {
		t.Fatal("event-time dependency inventory", page, err)
	}
	scan.AfterVersionID = original.VersionID
	end, err := engine.ScanMemoryRefresh(ctx, c, scan)
	if err != nil || len(end.Versions) != 0 {
		t.Fatal("dependency pagination", end, err)
	}
	scan.AfterVersionID = ""
	scan.Trigger.LedgerRevision++
	if _, err = engine.ScanMemoryRefresh(ctx, c, scan); !errors.Is(err, model.ErrDenied) {
		t.Fatal("forged trigger revision accepted", err)
	}
	scan.Trigger = *scheduled.Request.RefreshTrigger
	denied, err := facts.New(l, denyPermission{permission: "can_read_history"}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = denied.ScanMemoryRefresh(ctx, c, scan); !errors.Is(err, model.ErrDenied) {
		t.Fatal("dependency scan permission bypassed", err)
	}
	unrelated, err := engine.ListChanges(ctx, c, model.ChangesQuery{Scope: "case1", Purpose: r.Purpose, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	restricted, err := facts.New(l, denyScope{scope: "case1"}, store)
	if err != nil {
		t.Fatal(err)
	}
	checkedUnrelated := false
	for _, event := range unrelated.Events {
		if !model.ChangesMemoryDependency(event) {
			continue
		}
		irrelevant := scan
		irrelevant.Trigger = model.MemoryRefreshTrigger{EventID: event.ID, LedgerRevision: event.LedgerRevision}
		empty, err := restricted.ScanMemoryRefresh(ctx, c, irrelevant)
		if err != nil || len(empty.Versions) != 0 {
			t.Fatal("unrelated source scope blocked scheduler", empty, err)
		}
		checkedUnrelated = true
		break
	}
	if !checkedUnrelated {
		t.Fatal("unrelated event fixture missing")
	}
	other := c
	other.Tenant = "other-org"
	if _, err = engine.ScanMemoryRefresh(ctx, other, scan); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross-tenant trigger accepted", err)
	}
	if err = handler.Handle(ctx, model.Delivery{Key: key, Event: trigger, Fence: 999}); err != nil {
		t.Fatal("scheduler replay failed", err)
	}
	replay, err := engine.ListChanges(ctx, c, model.ChangesQuery{Scope: r.Scope, Purpose: r.Purpose, Limit: 1000})
	if err != nil || replay.ThroughRevision != changes.ThroughRevision {
		t.Fatal("replayed scheduling appended duplicate job", replay, err)
	}
	job, err := engine.GetJob(ctx, c, model.JobQuery{ID: scheduled.ID, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil || job.Kind != "memory_refresh" || job.State != "pending" {
		t.Fatal(job, err)
	}
	testRemoteRefreshAdministration(t, ctx, engine, denied, c, scan, r.ProfileID)
	return job
}
