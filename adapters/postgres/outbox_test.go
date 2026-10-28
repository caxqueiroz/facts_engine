package postgres_test

import (
	"bytes"
	"context"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"testing"
	"time"
)

func testOutbox(t *testing.T, ctx context.Context, l *postgres.Ledger, admin *pgxpool.Pool) {
	tenant := "outbox-org"
	for _, id := range []string{"event-a", "event-b", "event-c"} {
		err := l.Transact(ctx, tenant, func(tx model.Tx) error {
			eventType := "test"
			if id == "event-a" {
				eventType = "put_scope"
				if err := tx.(model.IdentityTx).StageScope(ctx, model.ScopeVersion{Definition: model.ScopeDefinition{ID: "scope1", Label: "Outbox test"}, Revision: 1, Caller: model.Caller{Tenant: tenant, Principal: "operator"}, Reason: "Test fixture"}, 0); err != nil {
					return err
				}
			}
			_, err := tx.Append(ctx, model.Event{ID: id, Type: eventType, Scope: "scope1", Caller: model.Caller{Tenant: tenant, Principal: "operator"}})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	workerConfig := admin.Config().Copy()
	workerConfig.ConnConfig.RuntimeParams["role"] = "facts_worker"
	workerPool, err := pgxpool.NewWithConfig(ctx, workerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer workerPool.Close()
	cipher, err := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	if err != nil {
		t.Fatal(err)
	}
	l, err = postgres.New(ctx, workerPool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"UPDATE ledger_events SET event_type='tampered'", "DELETE FROM outbox", "TRUNCATE consumer_retry_commands"} {
		if _, err := workerPool.Exec(ctx, query); err == nil {
			t.Fatal("worker privilege escaped", query)
		}
	}
	k := model.ConsumerKey{Tenant: tenant, Consumer: "projection"}
	if err := l.RegisterConsumer(ctx, k, 2); err != nil {
		t.Fatal(err)
	}
	if err := l.RegisterConsumer(ctx, k, 3); !errors.Is(err, model.ErrConflict) {
		t.Fatal("configuration changed", err)
	}
	status := func() model.ConsumerStatus {
		t.Helper()
		s, err := l.ConsumerStatus(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	claim := func() model.Delivery {
		t.Helper()
		d, err := l.ClaimDelivery(ctx, k, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	expire := func() {
		t.Helper()
		if _, err := admin.Exec(ctx, `UPDATE consumer_offsets SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND consumer_id=$2`, k.Tenant, k.Consumer); err != nil {
			t.Fatal(err)
		}
	}
	first := claim()
	if first.Event.ID != "event-a" || first.Attempt != 1 {
		t.Fatal(first)
	}
	if _, err := l.ClaimDelivery(ctx, k, time.Minute); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("live lease stolen", err)
	}
	if err := l.RenewDelivery(ctx, first, time.Minute); err != nil {
		t.Fatal(err)
	}
	forged := first
	forged.Event.LedgerRevision = 2
	forged.Event.ID = "event-b"
	if err := l.AcknowledgeDelivery(ctx, forged); !errors.Is(err, model.ErrRevision) {
		t.Fatal("gap acknowledged", err)
	}
	// External effect succeeded, then the process died before acknowledgement.
	effects := map[string]int{first.Event.ID: 1}
	expire()
	second := claim()
	if second.Fence <= first.Fence || second.Event.ID != first.Event.ID || second.Attempt != 2 {
		t.Fatal(second)
	}
	for _, fn := range []func() error{
		func() error { return l.AcknowledgeDelivery(ctx, first) },
		func() error { return l.RenewDelivery(ctx, first, time.Minute) },
		func() error { return l.FailDelivery(ctx, first, time.Second) },
	} {
		if err := fn(); !errors.Is(err, model.ErrRevision) {
			t.Fatal("stale worker mutation", err)
		}
	}
	// An idempotent consumer reconciles the duplicate external effect.
	if effects[second.Event.ID] == 0 {
		effects[second.Event.ID]++
	}
	if err := l.AcknowledgeDelivery(ctx, second); err != nil {
		t.Fatal(err)
	}
	if s := status(); s.AcknowledgedRevision != 1 || s.Attempts != 0 || effects["event-a"] != 1 {
		t.Fatal(s, effects)
	}
	next := claim()
	if next.Event.ID != "event-b" {
		t.Fatal(next)
	}
	if err := l.FailDelivery(ctx, next, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := l.ClaimDelivery(ctx, k, time.Minute); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("backoff ignored", err)
	}
	if _, err := admin.Exec(ctx, `UPDATE consumer_offsets SET available_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND consumer_id=$2`, k.Tenant, k.Consumer); err != nil {
		t.Fatal(err)
	}
	next = claim()
	if err := l.FailDelivery(ctx, next, time.Second); err != nil {
		t.Fatal(err)
	}
	if s := status(); s.State != "failed" || s.AcknowledgedRevision != 1 {
		t.Fatal(s)
	}
	if _, err := l.ClaimDelivery(ctx, k, time.Minute); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("failed event skipped", err)
	}
	if err := l.RetryConsumer(ctx, k, next.Fence-1); !errors.Is(err, model.ErrRevision) {
		t.Fatal("stale retry", err)
	}
	if err := l.RetryConsumer(ctx, k, next.Fence); err != nil {
		t.Fatal(err)
	}
	if err := l.RetryConsumer(ctx, k, next.Fence); err != nil {
		t.Fatal("retry response loss", err)
	}
	// Parallel claimants cannot both own the next event.
	var wg sync.WaitGroup
	outcomes := make(chan model.Delivery, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := l.ClaimDelivery(ctx, k, time.Minute)
			if err == nil {
				outcomes <- d
			} else {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(outcomes)
	close(errs)
	if len(outcomes) != 1 {
		t.Fatal("multiple lease owners", len(outcomes))
	}
	for err := range errs {
		if !errors.Is(err, model.ErrNotFound) {
			t.Fatal(err)
		}
	}
	claimed := <-outcomes
	if err := l.AcknowledgeDelivery(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	// The public worker invokes handlers outside transactions and acknowledges.
	handled := 0
	w, err := workers.NewOutbox(l, k, workers.HandlerFunc(func(ctx context.Context, d model.Delivery) error {
		handled++
		if d.Event.ID != "event-c" {
			t.Fatal(d)
		}
		_, err := l.ConsumerStatus(ctx, k)
		return err
	}), workers.Options{Lease: 3 * time.Second, PollInterval: time.Millisecond, MinBackoff: time.Second, MaxBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := w.ProcessOne(ctx); !worked || err != nil {
		t.Fatal(worked, err)
	}
	if worked, err := w.ProcessOne(ctx); worked || err != nil {
		t.Fatal(worked, err)
	}
	if s := status(); s.AcknowledgedRevision != 3 || handled != 1 {
		t.Fatal(s, handled)
	}
	// Archive has its own independent progress, and another tenant cannot see it.
	archive := model.ConsumerKey{Tenant: tenant, Consumer: "archive"}
	if err := l.RegisterConsumer(ctx, archive, 1); err != nil {
		t.Fatal(err)
	}
	a, err := l.ClaimDelivery(ctx, archive, time.Second)
	if err != nil || a.Event.ID != "event-a" {
		t.Fatal(a, err)
	}
	if _, err := l.ConsumerStatus(ctx, model.ConsumerKey{Tenant: "another-org", Consumer: "archive"}); !errors.Is(err, model.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `UPDATE consumer_offsets SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND consumer_id=$2`, archive.Tenant, archive.Consumer); err != nil {
		t.Fatal(err)
	}
	if _, err := l.ClaimDelivery(ctx, archive, time.Second); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("crash budget ignored", err)
	}
	s, err := l.ConsumerStatus(ctx, archive)
	if err != nil || s.State != "failed" || s.AcknowledgedRevision != 0 {
		t.Fatal(s, err)
	}
	// A damaged/missing outbox row must never be mistaken for a caught-up stream.
	gapKey := model.ConsumerKey{Tenant: tenant, Consumer: "gap-detector"}
	if err := l.RegisterConsumer(ctx, gapKey, 2); err != nil {
		t.Fatal(err)
	}
	gapDelivery, err := l.ClaimDelivery(ctx, gapKey, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.AcknowledgeDelivery(ctx, gapDelivery); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `DELETE FROM outbox WHERE tenant_id=$1 AND ledger_revision=2`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := l.ClaimDelivery(ctx, gapKey, time.Minute); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("missing event silently skipped", err)
	}
	// Authorized replay is paginated by revision and does not mutate offsets.
	engine, err := facts.New(l, allow{}, unusedEvidence{})
	if err != nil {
		t.Fatal(err)
	}
	caller := model.Caller{Tenant: tenant, Principal: "reader"}
	page, err := engine.ListChanges(ctx, caller, model.ChangesQuery{Scope: "scope1", Purpose: "replay", Limit: 2})
	if err != nil || len(page.Events) != 2 || page.NextRevision != 2 {
		t.Fatal(page, err)
	}
	page, err = engine.ListChanges(ctx, caller, model.ChangesQuery{Scope: "scope1", Purpose: "replay", AfterRevision: page.NextRevision, Limit: 2})
	if err != nil || len(page.Events) != 1 || page.NextRevision != 3 {
		t.Fatal(page, err)
	}
	denied, _ := facts.New(l, denyPermission{permission: "can_read_history"}, unusedEvidence{})
	if _, err := denied.ListChanges(ctx, caller, model.ChangesQuery{Scope: "scope1", Purpose: "replay", Limit: 2}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("unauthorized replay", err)
	}
	page, err = engine.ListChanges(ctx, caller, model.ChangesQuery{Scope: "scope2", Purpose: "replay", Limit: 2})
	if err != nil || len(page.Events) != 0 || page.NextRevision != 3 {
		t.Fatal("cross-scope replay", page, err)
	}
}

type unusedEvidence struct{}

func (unusedEvidence) Read(context.Context, string, model.ObjectRef) ([]byte, error) {
	return nil, model.ErrNotFound
}
