package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) loadMemoryRefreshEvent(ctx context.Context, c model.Caller, q model.MemoryRefreshTrigger, purpose string) (model.Event, error) {
	if !model.ValidID(q.EventID) || q.LedgerRevision < 1 {
		return model.Event{}, model.ErrInvalid
	}
	var event model.Event
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, ok := tx.(model.MemoryRefreshScanTx)
		if !ok {
			return model.ErrPrecondition
		}
		var err error
		event, err = t.MemoryRefreshEvent(ctx, q)
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		return event, model.ErrDenied
	}
	if err != nil {
		return event, err
	}
	if !model.ChangesMemoryDependency(event) && !model.IsMemoryRefreshSweep(event) {
		return model.Event{}, model.ErrInvalid
	}
	return event, nil
}

func (s *Service) memoryRefreshEvent(ctx context.Context, c model.Caller, q model.MemoryRefreshTrigger, purpose string) (model.Event, error) {
	event, err := s.loadMemoryRefreshEvent(ctx, c, q, purpose)
	if err != nil {
		return event, err
	}
	for _, permission := range []string{"can_read", "can_read_history"} {
		if _, err = s.authorize(ctx, c, event.Scope, permission, model.Command{Purpose: purpose}); err != nil {
			return model.Event{}, err
		}
	}
	return event, nil
}

// ScanMemoryRefresh is a bounded operational scan over the immutable event-time
// dependency inventory. Each page independently authorizes source and target.
func (s *Service) ScanMemoryRefresh(ctx context.Context, c model.Caller, q model.MemoryRefreshScan) (model.MemoryRefreshScanResult, error) {
	if q.Limit < 1 || q.Limit > 100 || (q.AfterVersionID != "" && !model.ValidID(q.AfterVersionID)) {
		return model.MemoryRefreshScanResult{}, model.ErrInvalid
	}
	check := func() error {
		for _, permission := range []string{"can_extract", "can_read", "can_read_history"} {
			if _, err := s.authorize(ctx, c, q.Scope, permission, model.Command{Purpose: q.Purpose}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := check(); err != nil {
		return model.MemoryRefreshScanResult{}, err
	}
	event, err := s.loadMemoryRefreshEvent(ctx, c, q.Trigger, q.Purpose)
	if err != nil {
		return model.MemoryRefreshScanResult{}, err
	}
	if model.IsMemoryRefreshSweep(event) && event.Scope != q.Scope {
		return model.MemoryRefreshScanResult{}, model.ErrDenied
	}
	var out model.MemoryRefreshScanResult
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, ok := tx.(model.MemoryRefreshScanTx)
		if !ok {
			return model.ErrPrecondition
		}
		var err error
		out.Versions, err = t.MemoryRefreshDependents(ctx, q.Scope, event.RecordID, q.Trigger.LedgerRevision, q.AfterVersionID, q.Limit)
		return err
	})
	if err != nil {
		return model.MemoryRefreshScanResult{}, err
	}
	if err = check(); err != nil {
		return model.MemoryRefreshScanResult{}, err
	}
	if len(out.Versions) > 0 {
		if _, err = s.memoryRefreshEvent(ctx, c, q.Trigger, q.Purpose); err != nil {
			return model.MemoryRefreshScanResult{}, err
		}
	}
	return out, nil
}
