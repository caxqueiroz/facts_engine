package workers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

type extractionRunner struct {
	calls   int
	request model.ExtractionRunRequest
	state   string
	err     error
}

func (r *extractionRunner) GetJob(context.Context, model.Caller, model.JobQuery) (model.JobStatus, error) {
	return model.JobStatus{ID: "job", Scope: "scope", State: "pending", Purpose: "original-purpose"}, r.err
}
func (r *extractionRunner) RunExtraction(_ context.Context, _ model.Caller, q model.ExtractionRunRequest) (model.JobStatus, error) {
	r.calls++
	r.request = q
	return model.JobStatus{ID: "job", Scope: "scope", State: r.state}, r.err
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

type timedExtractionRunner struct {
	before, after, result model.JobStatus
	reads, calls          int
	runErr                error
}

func (r *timedExtractionRunner) GetJob(context.Context, model.Caller, model.JobQuery) (model.JobStatus, error) {
	r.reads++
	if r.reads > 1 {
		return r.after, nil
	}
	return r.before, nil
}
func (r *timedExtractionRunner) RunExtraction(context.Context, model.Caller, model.ExtractionRunRequest) (model.JobStatus, error) {
	r.calls++
	return r.result, r.runErr
}
func TestExtractionRespectsDurableRetryTime(t *testing.T) {
	for _, scenario := range []string{"pending delay", "active lease", "provider retry", "failed commit lease"} {
		t.Run(scenario, func(t *testing.T) {
			future := time.Now().Add(2 * time.Minute)
			pending := model.JobStatus{ID: "job", Scope: "scope", State: "pending", Purpose: "original-purpose"}
			delayed := pending
			delayed.AvailableAt = future
			running := pending
			running.State = "running"
			running.LeaseUntil = &future
			runner := &timedExtractionRunner{before: pending, result: delayed}
			wantCalls := 1
			wantCause := model.ErrUnavailable
			switch scenario {
			case "pending delay":
				runner.before = delayed
				wantCalls = 0
			case "active lease":
				runner.before = running
				wantCalls = 0
			case "failed commit lease":
				runner.runErr = model.ErrConflict
				runner.after = running
				wantCause = model.ErrConflict
			}
			caller := model.Caller{Tenant: "tenant", Principal: "worker", Service: true}
			handler, err := NewExtraction(runner, caller, "v1")
			if err != nil {
				t.Fatal(err)
			}
			delivery := model.Delivery{Key: model.ConsumerKey{Tenant: "tenant", Consumer: model.ExtractionConsumer("v1")}, Fence: 1, Event: model.Event{ID: "event", EntityID: "job", Scope: "scope", Type: "start_extraction", Caller: caller}}
			err = handler.Handle(context.Background(), delivery)
			var retry *retryAfterError
			if !errors.Is(err, wantCause) || !errors.As(err, &retry) || retry.delay < 90*time.Second || runner.calls != wantCalls {
				t.Fatal("retry timing lost", err, runner.calls)
			}
			queue := &queueFake{}
			worker := workerFor(t, queue, func(context.Context, model.Delivery) error { return err })
			if worked, runErr := worker.ProcessOne(context.Background()); !worked || runErr != nil {
				t.Fatal(worked, runErr)
			}
			if queue.acks != 0 || queue.failures != 1 || queue.delay < 90*time.Second {
				t.Fatal("outbox ignored job delay", queue.delay)
			}
		})
	}
}
func TestExtractionStatusBindingAndTerminalReplay(t *testing.T) {
	caller := model.Caller{Tenant: "tenant", Principal: "worker", Service: true}
	delivery := model.Delivery{Key: model.ConsumerKey{Tenant: "tenant", Consumer: model.ExtractionConsumer("v1")}, Fence: 1, Event: model.Event{ID: "event", EntityID: "job", Scope: "scope", Type: "start_extraction", Caller: caller}}
	for _, state := range []string{"completed", "failed"} {
		runner := &timedExtractionRunner{before: model.JobStatus{ID: "job", Scope: "scope", State: state}}
		handler, _ := NewExtraction(runner, caller, "v1")
		if err := handler.Handle(context.Background(), delivery); err != nil || runner.calls != 0 {
			t.Fatal("terminal job invoked executor", err)
		}
	}
	for _, stage := range []string{"before", "after"} {
		for _, invalid := range []model.JobStatus{{ID: "other", Scope: "scope", State: "completed"}, {ID: "job", Scope: "other", State: "completed"}, {ID: "job", Scope: "scope", State: "unknown"}} {
			runner := &timedExtractionRunner{before: model.JobStatus{ID: "job", Scope: "scope", State: "pending"}, result: invalid}
			if stage == "before" {
				runner.before = invalid
			}
			handler, _ := NewExtraction(runner, caller, "v1")
			if err := handler.Handle(context.Background(), delivery); !errors.Is(err, model.ErrPrecondition) {
				t.Fatal("invalid job status accepted", stage, err)
			}
		}
	}
}
