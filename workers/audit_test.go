package workers

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

type auditRunner struct {
	caller model.Caller
	query  model.AuditExportQuery
	calls  int
	err    error
}

func (r *auditRunner) RunAuditExport(_ context.Context, c model.Caller, q model.AuditExportQuery) (model.AuditExportResult, error) {
	r.caller = c
	r.query = q
	r.calls++
	return model.AuditExportResult{ID: q.ID, State: "completed"}, r.err
}

type auditAuthorizer struct {
	deny     bool
	stale    bool
	requests []model.AuthorizationRequest
}

func (a *auditAuthorizer) Check(_ context.Context, r model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	a.requests = append(a.requests, r)
	checked := time.Now()
	if a.stale {
		checked = checked.Add(-time.Minute)
	}
	return model.AuthorizationDecision{Allowed: !a.deny, ModelID: "model", Consistency: "HIGHER_CONSISTENCY", CheckedAt: checked}, nil
}
func TestAuditDeliveryIdentityAndLeaseRetry(t *testing.T) {
	worker := model.Caller{Tenant: "tenant", Principal: "worker", Service: true}
	requester := model.Caller{Tenant: "tenant", Principal: "requester", Service: true, Initiator: "human"}
	runner := &auditRunner{}
	auth := &auditAuthorizer{}
	handler, err := NewAuditExport(runner, auth, worker, "stream")
	if err != nil {
		t.Fatal(err)
	}
	d := model.Delivery{Key: model.ConsumerKey{Tenant: "tenant", Consumer: model.AuditExportConsumer("stream")}, Fence: 1, Event: model.Event{ID: "event", EntityID: "export", Type: "start_audit_export", Scope: "scope", Caller: requester, Command: model.Command{Purpose: "original-purpose"}}}
	if err := handler.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if runner.caller != requester || runner.query != (model.AuditExportQuery{ID: "export", Scope: "scope", Purpose: "original-purpose"}) || len(auth.requests) != 2 {
		t.Fatal("requester identity or worker authorization lost")
	}
	for _, request := range auth.requests {
		if request.Caller != worker {
			t.Fatal("worker check used requester")
		}
	}
	auth.deny = true
	if err := handler.Handle(context.Background(), d); !errors.Is(err, model.ErrDenied) || runner.calls != 1 {
		t.Fatal("worker denial bypassed", err)
	}
	auth.deny = false
	auth.stale = true
	if err := handler.Handle(context.Background(), d); !errors.Is(err, model.ErrUnavailable) || runner.calls != 1 {
		t.Fatal("stale worker authorization", err)
	}
	auth.stale = false
	runner.err = model.ErrDenied
	err = handler.Handle(context.Background(), d)
	var retry *retryAfterError
	if !errors.Is(err, model.ErrDenied) || !errors.As(err, &retry) || retry.delay != model.AuditExportLease {
		t.Fatalf("requester denial retry: %v", err)
	}
	queue := &queueFake{}
	outbox := workerFor(t, queue, func(context.Context, model.Delivery) error { return err })
	if worked, err := outbox.ProcessOne(context.Background()); err != nil || !worked {
		t.Fatal(err)
	}
	if queue.acks != 0 || queue.failures != 1 || queue.delay < model.AuditExportLease {
		t.Fatal("job lease bypassed by delivery retry", queue.delay)
	}
	calls := runner.calls
	d.Event.Type = "complete_audit_export"
	if err := handler.Handle(context.Background(), d); err != nil || runner.calls != calls {
		t.Fatal("completion recursed", err)
	}
	d.Key.Tenant = "other"
	if err := handler.Handle(context.Background(), d); !errors.Is(err, model.ErrDenied) {
		t.Fatal("tenant binding", err)
	}
}
