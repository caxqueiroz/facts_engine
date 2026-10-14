package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

func (s *Service) snapshotAuthorization(ctx context.Context, c model.Caller, keys []model.FactKey, cmd model.Command, historical, create bool) ([]model.AuthorizationDecision, error) {
	receipts := []model.AuthorizationDecision{}
	scopes := map[string]bool{}
	for _, key := range keys {
		if scopes[key.Scope] {
			continue
		}
		scopes[key.Scope] = true
		permissions := []string{"can_read"}
		if create {
			permissions = append(permissions, "can_snapshot")
		}
		if historical {
			permissions = append(permissions, "can_read_history")
		}
		for _, permission := range permissions {
			d, err := s.authorize(ctx, c, key.Scope, permission, cmd)
			if err != nil {
				return nil, err
			}
			receipts = append(receipts, d)
		}
	}
	return receipts, nil
}
func (s *Service) CreateFactSnapshot(ctx context.Context, c model.Caller, r model.FactSnapshotRequest) (model.FactSnapshot, error) {
	if len(r.Selectors) == 0 || len(r.Selectors) > 128 || r.ValidAt.IsZero() || r.KnowledgeRevision < 0 || !model.ValidID(r.Key) {
		return model.FactSnapshot{}, model.ErrInvalid
	}
	seen := map[model.FactKey]bool{}
	for _, key := range r.Selectors {
		if !validSelector(key) || seen[key] {
			return model.FactSnapshot{}, model.ErrInvalid
		}
		seen[key] = true
	}
	r.ValidAt = r.ValidAt.UTC()
	receipts, err := s.snapshotAuthorization(ctx, c, r.Selectors, r.Command, r.KnowledgeRevision > 0, true)
	if err != nil {
		return model.FactSnapshot{}, err
	}
	snapshot := model.FactSnapshot{ID: id(), Tenant: c.Tenant, Caller: c, Purpose: r.Purpose, ValidAt: r.ValidAt, CanonicalVersion: "fact-snapshot-v1", Authorization: receipts}
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		rev, at, err := selection(ctx, tx, r.KnowledgeRevision)
		if err != nil {
			return err
		}
		snapshot.SelectionRevision = rev
		snapshot.EvaluatedAt = at
		snapshot.Items = make([]model.SnapshotItem, 0, len(r.Selectors))
		for _, key := range r.Selectors {
			item, err := s.resolve(ctx, tx, model.FactQuery{Key: key, ValidAt: r.ValidAt}, rev, at, r.KnowledgeRevision > 0)
			if err != nil {
				return err
			}
			snapshot.Items = append(snapshot.Items, item)
		}
		return nil
	})
	if err != nil {
		return model.FactSnapshot{}, err
	}
	for i := range snapshot.Items {
		item := &snapshot.Items[i]
		if len(item.EvidenceIDs) == 0 {
			continue
		}
		if _, err := s.loadEvidence(ctx, c, item.Selector.Scope, item.EvidenceIDs); err != nil {
			item.Resolution.State = "ineligible"
			item.Resolution.Value = nil
		}
	}
	snapshot.Digest = model.SnapshotDigest(snapshot)
	result, err := s.mutate(ctx, c, r.Selectors[0].Scope, "create_fact_snapshot", "can_snapshot", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		for _, receipt := range receipts {
			if time.Since(receipt.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		t, err := temporal(tx)
		if err != nil {
			return err
		}
		if err := t.StageSnapshot(ctx, snapshot); err != nil {
			return err
		}
		out.ID = snapshot.ID
		out.ContentDigest = snapshot.Digest
		return nil
	})
	if err != nil {
		return model.FactSnapshot{}, err
	}
	return s.GetFactSnapshot(ctx, c, model.SnapshotQuery{ID: result.ID, Purpose: r.Purpose})
}
func (s *Service) GetFactSnapshot(ctx context.Context, c model.Caller, q model.SnapshotQuery) (model.FactSnapshot, error) {
	if !model.ValidID(c.Tenant) || !model.ValidID(c.Principal) || !model.ValidID(q.ID) || q.Purpose == "" {
		return model.FactSnapshot{}, model.ErrInvalid
	}
	var snapshot model.FactSnapshot
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := temporal(tx)
		if err != nil {
			return err
		}
		snapshot, err = t.Snapshot(ctx, q.ID)
		return err
	})
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			return model.FactSnapshot{}, model.ErrDenied
		}
		return model.FactSnapshot{}, err
	}
	keys := make([]model.FactKey, 0, len(snapshot.Items))
	for _, item := range snapshot.Items {
		keys = append(keys, item.Selector)
	}
	if _, err := s.snapshotAuthorization(ctx, c, keys, model.Command{Purpose: q.Purpose}, false, false); err != nil {
		return model.FactSnapshot{}, err
	}
	if snapshot.Tenant != c.Tenant || model.SnapshotDigest(snapshot) != snapshot.Digest {
		return model.FactSnapshot{}, model.ErrPrecondition
	}
	return snapshot, nil
}
