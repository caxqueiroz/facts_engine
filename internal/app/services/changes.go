package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

// ListChanges is scope-authorized replay, not a consumer acknowledgement API.
func (s *Service) ListChanges(ctx context.Context, c model.Caller, q model.ChangesQuery) (model.ChangesResult, error) {
	result := model.ChangesResult{}
	if q.AfterRevision < 0 || q.Limit < 1 || q.Limit > 1000 {
		return result, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, q.Scope, "can_read_history", model.Command{Purpose: q.Purpose}); err != nil {
		return result, err
	}
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, ok := tx.(model.ChangesTx)
		if !ok {
			return model.ErrPrecondition
		}
		var err error
		result.ThroughRevision, err = tx.Revision(ctx)
		if err != nil {
			return err
		}
		if q.AfterRevision > result.ThroughRevision {
			return model.ErrInvalid
		}
		result.Events, err = t.Changes(ctx, q.Scope, q.AfterRevision, result.ThroughRevision, q.Limit)
		if err != nil {
			return err
		}
		result.NextRevision = result.ThroughRevision
		if len(result.Events) == q.Limit {
			result.NextRevision = result.Events[len(result.Events)-1].LedgerRevision
		}
		return nil
	})
	return result, err
}
