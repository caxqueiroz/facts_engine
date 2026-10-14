package workers

import (
	"context"

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
	case "start_extraction", "retry_extraction":
	default:
		return nil
	}
	if d.Fence < 1 || !model.ValidID(d.Event.ID) || !model.ValidID(d.Event.EntityID) {
		return model.ErrInvalid
	}
	status, err := e.runner.GetJob(ctx, e.caller, model.JobQuery{ID: d.Event.EntityID, Scope: d.Event.Scope, Purpose: d.Event.Command.Purpose})
	if err != nil {
		return err
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
		return err
	}
	switch status.State {
	case "completed", "failed":
		return nil
	default:
		// Keep this notification unacknowledged until the job is terminal. The
		// outbox owns delivery backoff; the engine independently enforces job delay.
		return model.ErrUnavailable
	}
}
