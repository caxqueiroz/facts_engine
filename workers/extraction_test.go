package workers

import (
	"context"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

type extractionRunner struct {
	calls   int
	request model.ExtractionRunRequest
	state   string
	err     error
}

func (r *extractionRunner) GetJob(context.Context, model.Caller, model.JobQuery) (model.JobStatus, error) {
	return model.JobStatus{Purpose: "original-purpose"}, r.err
}
func (r *extractionRunner) RunExtraction(_ context.Context, _ model.Caller, q model.ExtractionRunRequest) (model.JobStatus, error) {
	r.calls++
	r.request = q
	return model.JobStatus{State: r.state}, r.err
}
func TestExtractionDelivery(t *testing.T) {
	ctx := context.Background()
	caller := model.Caller{Tenant: "tenant", Principal: "worker", Service: true}
	runner := &extractionRunner{state: "completed"}
	handler, err := NewExtraction(runner, caller, "v1")
	if err != nil {
		t.Fatal(err)
	}
	d := model.Delivery{Key: model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.ExtractionConsumer("v1")}, Fence: 1, Event: model.Event{ID: "event", EntityID: "job", Type: "retry_extraction", Scope: "scope", Caller: caller, Command: model.Command{Purpose: "operator-retry"}}}
	if err = handler.Handle(ctx, d); err != nil || runner.request.Purpose != "original-purpose" || runner.request.ID != "job" {
		t.Fatal(runner, err)
	}
	first := runner.request.Key
	if err = handler.Handle(ctx, d); err != nil || runner.request.Key != first {
		t.Fatal("unstable delivery key", err)
	}
	d.Fence++
	if err = handler.Handle(ctx, d); err != nil || runner.request.Key == first {
		t.Fatal("retry did not get a fresh invocation", err)
	}
	for _, state := range []string{"pending", "running", "failed", "completed"} {
		runner.state = state
		err = handler.Handle(ctx, d)
		if (state == "pending" || state == "running") != errors.Is(err, model.ErrUnavailable) {
			t.Fatal(state, err)
		}
	}
	runner.err = model.ErrDenied
	if err = handler.Handle(ctx, d); !errors.Is(err, model.ErrDenied) {
		t.Fatal(err)
	}
	runner.err = nil
	count := runner.calls
	d.Event.Type = "propose_fact"
	if err = handler.Handle(ctx, d); err != nil || runner.calls != count {
		t.Fatal("unrelated event invoked provider", err)
	}
	d.Key.Tenant = "other"
	if err = handler.Handle(ctx, d); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross-tenant delivery", err)
	}
	caller.Service = false
	if _, err = NewExtraction(runner, caller, "v1"); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("non-service worker accepted", err)
	}
}
