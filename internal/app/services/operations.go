package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
)

func (s *Service) operationsAuthorization(ctx context.Context, c model.Caller, purpose string) (model.AuthorizationDecision, error) {
	if s.operations.Store == nil {
		return model.AuthorizationDecision{}, model.ErrUnavailable
	}
	return s.authorize(ctx, c, s.operations.Scope, "can_operate", model.Command{Purpose: purpose})
}
func (s *Service) GetProjectionStatus(ctx context.Context, c model.Caller, q model.ProjectionStatusQuery) (model.ProjectionStatus, error) {
	empty := model.ProjectionStatus{}
	if q.Generation != "" && !model.ValidID(q.Generation) {
		return empty, model.ErrInvalid
	}
	if _, err := s.operationsAuthorization(ctx, c, q.Purpose); err != nil {
		return empty, err
	}
	active, err := s.operations.Store.ActiveProjection(ctx, c.Tenant)
	if err != nil {
		return empty, err
	}
	generation := q.Generation
	if generation == "" {
		generation = active.Generation
	}
	if generation == "" {
		return empty, model.ErrUnavailable
	}
	out := model.ProjectionStatus{Active: active}
	out.Rebuild, err = s.operations.Store.RebuildStatus(ctx, model.GenerationKey{Tenant: c.Tenant, Generation: generation})
	if err != nil {
		return empty, err
	}
	out.Consumer, err = s.operations.Progress.ConsumerStatus(ctx, model.ConsumerKey{Tenant: c.Tenant, Consumer: model.ProjectionConsumer(generation)})
	if err != nil {
		return empty, err
	}
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error { var err error; out.LedgerRevision, err = tx.Revision(ctx); return err })
	if err != nil {
		return empty, err
	}
	// Never combine progress with an active pointer that changed during the read.
	current, err := s.operations.Store.ActiveProjection(ctx, c.Tenant)
	if err != nil {
		return empty, err
	}
	if current != active {
		return empty, model.ErrUnavailable
	}
	if _, err := s.operationsAuthorization(ctx, c, q.Purpose); err != nil {
		return empty, err
	}
	out.Incomplete = out.Consumer.AcknowledgedRevision < out.LedgerRevision || out.Consumer.State == "failed" || generation != active.Generation || out.Rebuild.State != "active"
	return out, nil
}
func (s *Service) RebuildIndex(ctx context.Context, c model.Caller, r model.RebuildRequest) (model.RebuildStatus, error) {
	empty := model.RebuildStatus{}
	if !model.ValidID(r.Generation) {
		return empty, model.ErrInvalid
	}
	switch r.Action {
	case "start":
		if r.MaxAttempts < 1 || r.MaxAttempts > 100 || r.ExpectedFence != 0 {
			return empty, model.ErrInvalid
		}
	case "verify":
		if r.MaxAttempts != 0 || r.ExpectedFence != 0 {
			return empty, model.ErrInvalid
		}
	case "resume", "activate", "abandon":
		if r.MaxAttempts != 0 || r.ExpectedFence < 1 {
			return empty, model.ErrInvalid
		}
	default:
		return empty, model.ErrInvalid
	}
	auth, err := s.operationsAuthorization(ctx, c, r.Purpose)
	if err != nil {
		return empty, err
	}
	manager, err := workers.NewRebuild(s.operations.Store, s.operations.Index, model.RebuildActor{Principal: c.Principal, Purpose: r.Purpose, Caller: &c, Authorization: &auth})
	if err != nil {
		return empty, err
	}
	key := model.GenerationKey{Tenant: c.Tenant, Generation: r.Generation}
	switch r.Action {
	case "start":
		return manager.Start(ctx, key, r.MaxAttempts)
	case "verify":
		return manager.Verify(ctx, key)
	case "resume":
		return manager.Resume(ctx, key, r.ExpectedFence)
	case "activate":
		return manager.Activate(ctx, key, r.ExpectedFence)
	default:
		return manager.Abandon(ctx, key, r.ExpectedFence)
	}
}
