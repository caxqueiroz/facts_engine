package workers_test

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"time"
)

// runProjection demonstrates host-owned lifecycle and injected boundaries.
// RegisterConsumer and Run are explicit; the host closes pools after Run returns.
func runProjection(ctx context.Context, queue model.Outbox, source model.ProjectionSource, index model.RetrievalIndex, auth model.Authorizer, caller model.Caller, generation string) error {
	handler, err := workers.NewProjection(source, index, auth, caller, generation)
	if err != nil {
		return err
	}
	key := model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.ProjectionConsumer(generation)}
	if err := queue.RegisterConsumer(ctx, key, 8); err != nil {
		return err
	}
	worker, err := workers.NewOutbox(queue, key, handler, workers.Options{Lease: time.Minute, PollInterval: time.Second, MinBackoff: time.Second, MaxBackoff: time.Minute})
	if err != nil {
		return err
	}
	return worker.Run(ctx)
}
func ExampleNewProjection() {
	// The production host supplies a role-restricted ledger, a tenant-bound CortexDB
	// adapter, OpenFGA, an authenticated worker caller and a cancellable context.
	_ = runProjection
}
