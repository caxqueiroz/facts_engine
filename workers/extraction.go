package workers

import (
	"context"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

// Extraction routes durable job notifications through the engine. Job claims,
// authorization, provider calls and proposal commits remain engine-owned.
type Extraction struct {
	runner model.ExtractionRunner
	caller model.Caller
	stream string
}

func NewExtraction(runner model.ExtractionRunner, caller model.Caller, stream string) (*Extraction, error) {
	if runner == nil || !caller.Service || !model.ValidID(caller.Tenant) || !model.ValidID(caller.Principal) || !model.ValidID(stream) {
		return nil, model.ErrInvalid
	}
	return &Extraction{runner: runner, caller: caller, stream: stream}, nil
}

func (e *Extraction) Handle(ctx context.Context, d model.Delivery) error {
	if d.Key.Tenant != e.caller.Tenant || d.Key.Consumer != model.ExtractionConsumer(e.stream) || d.Event.Caller.Tenant != e.caller.Tenant {
		return model.ErrDenied
	}
	switch d.Event.Type {
	case "start_extraction", "start_memory_refresh", "retry_extraction":
	default:
		return nil
	}
	if d.Fence < 1 || !model.ValidID(d.Event.ID) || !model.ValidID(d.Event.EntityID) || !model.ValidID(d.Event.Scope) {
		return model.ErrInvalid
	}
	query := model.JobQuery{ID: d.Event.EntityID, Scope: d.Event.Scope, Purpose: d.Event.Command.Purpose}
	status, err := e.runner.GetJob(ctx, e.caller, query)
	if err != nil {
		return err
	}
	if err := extractionStatusBinding(status, query); err != nil {
		return err
	}
	if status.State == "completed" || status.State == "failed" {
		return nil
	}
	if delay := extractionRetryDelay(status); delay > 0 {
		return &retryAfterError{cause: model.ErrUnavailable, delay: delay}
	}
	// The operator's retry purpose may differ from the retained job purpose.
	// Provider processing always uses the original, authorized extraction purpose.
	key := model.Digest(struct {
		Event string
		Fence int64
	}{d.Event.ID, d.Fence})
	status, err = e.runner.RunExtraction(ctx, e.caller, model.ExtractionRunRequest{
		Command: model.Command{Key: key, Purpose: status.Purpose, CorrelationID: d.Event.Command.CorrelationID},
		ID:      d.Event.EntityID, Scope: d.Event.Scope,
	})
	if err != nil {
		// A provider or commit failure can leave a live job lease. Consult durable
		// status again so delivery retries cannot burn through the queue budget.
		if ctx.Err() == nil {
			latest, readErr := e.runner.GetJob(ctx, e.caller, query)
			if readErr == nil && extractionStatusBinding(latest, query) == nil {
				if delay := extractionRetryDelay(latest); delay > 0 {
					return &retryAfterError{cause: err, delay: delay}
				}
			}
		}
		return err
	}
	if err := extractionStatusBinding(status, query); err != nil {
		return err
	}
	switch status.State {
	case "completed", "failed":
		return nil
	default:
		// Queue backoff must not precede the engine's independently persisted delay.
		return &retryAfterError{cause: model.ErrUnavailable, delay: extractionRetryDelay(status)}
	}
}

func extractionStatusBinding(s model.JobStatus, q model.JobQuery) error {
	if s.ID != q.ID || s.Scope != q.Scope {
		return model.ErrPrecondition
	}
	switch s.State {
	case "pending", "running", "completed", "failed":
		return nil
	default:
		return model.ErrPrecondition
	}
}
func extractionRetryDelay(s model.JobStatus) time.Duration {
	var until time.Time
	switch s.State {
	case "pending":
		until = s.AvailableAt
	case "running":
		if s.LeaseUntil != nil {
			until = *s.LeaseUntil
		}
	}
	if delay := time.Until(until); delay > 0 {
		return delay + time.Millisecond
	}
	return 0
}
