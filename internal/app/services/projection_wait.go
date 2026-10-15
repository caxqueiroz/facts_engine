package services

import (
	"context"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) WaitForProjection(ctx context.Context, c model.Caller, q model.ProjectionWaitRequest) (model.ProjectionReceipt, error) {
	empty := model.ProjectionReceipt{}
	if _, ok := ctx.Deadline(); !ok {
		return empty, model.ErrInvalid
	}
	if !model.ValidID(q.Generation) || q.Revision < 1 || len(q.Scopes) == 0 || len(q.Scopes) > 8 {
		return empty, model.ErrInvalid
	}
	seen := map[string]bool{}
	for _, scope := range q.Scopes {
		if !model.ValidID(scope) || seen[scope] {
			return empty, model.ErrInvalid
		}
		seen[scope] = true
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	authorize := func() error {
		for _, scope := range q.Scopes {
			if _, err := s.authorize(ctx, c, scope, "can_read", model.Command{Purpose: q.Purpose}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := authorize(); err != nil {
		return empty, err
	}
	if s.retrieval.Index == nil || s.retrieval.Progress == nil {
		return empty, model.ErrUnavailable
	}
	var pinned model.ActiveProjection
	checkGeneration := func() error {
		if s.retrieval.Registry == nil {
			if s.retrieval.Generation != q.Generation {
				return model.ErrUnavailable
			}
			return nil
		}
		active, err := s.retrieval.Registry.ActiveProjection(ctx, c.Tenant)
		if failure := unavailableDependency(ctx, err); failure != nil {
			return failure
		}
		if active.Generation != q.Generation {
			return model.ErrUnavailable
		}
		if pinned.Generation != "" && active != pinned {
			return model.ErrUnavailable
		}
		pinned = active
		return nil
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		if err := checkGeneration(); err != nil {
			return empty, err
		}
		progress, err := s.retrieval.Progress.ConsumerStatus(ctx, model.ConsumerKey{Tenant: c.Tenant, Consumer: model.ProjectionConsumer(q.Generation)})
		if failure := unavailableDependency(ctx, err); failure != nil {
			return empty, failure
		}
		if progress.AcknowledgedRevision >= q.Revision {
			if err := authorize(); err != nil {
				return empty, err
			}
			if err := checkGeneration(); err != nil {
				return empty, err
			}
			return model.ProjectionReceipt{Generation: q.Generation, IndexedRevision: progress.AcknowledgedRevision}, nil
		}
		if progress.State == "failed" {
			return empty, model.ErrUnavailable
		}
		select {
		case <-ctx.Done():
			return empty, ctx.Err()
		case <-ticker.C:
		}
	}
}
