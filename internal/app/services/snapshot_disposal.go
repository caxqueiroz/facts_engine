package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"sort"
)

func (s *Service) DisposeSnapshot(ctx context.Context, c model.Caller, r model.SnapshotDisposalRequest) (model.CommitResult, error) {
	if !model.ValidID(r.SnapshotID) || r.Reason == "" || len(r.Reason) > 1024 || len(r.Bindings) < 1 || len(r.Bindings) > 256 {
		return model.CommitResult{}, model.ErrInvalid
	}
	r.Bindings = append([]model.SnapshotRetentionBinding(nil), r.Bindings...)
	sort.Slice(r.Bindings, func(i, j int) bool { return r.Bindings[i].Scope < r.Bindings[j].Scope })
	for i, b := range r.Bindings {
		if !model.ValidID(b.Scope) || !model.ValidID(b.PolicyID) || !model.ValidID(b.Jurisdiction) || (i > 0 && r.Bindings[i-1].Scope == b.Scope) {
			return model.CommitResult{}, model.ErrInvalid
		}
	}
	var receipts []model.AuthorizationDecision
	check := func() error {
		receipts = nil
		for _, b := range r.Bindings {
			d, err := s.authorize(ctx, c, b.Scope, "can_dispose", r.Command)
			if err != nil {
				return err
			}
			receipts = append(receipts, d)
		}
		return nil
	}
	if err := check(); err != nil {
		return model.CommitResult{}, err
	}
	return s.mutate(ctx, c, r.Bindings[0].Scope, "dispose_snapshot", "can_dispose", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		disposal, ok := tx.(model.SnapshotDisposalTx)
		if !ok {
			return model.ErrPrecondition
		}
		retention, err := retentionTx(tx)
		if err != nil {
			return err
		}
		if err := disposal.DisposeSnapshot(ctx, r); err != nil {
			return err
		}
		if err := check(); err != nil {
			return err
		}
		event := model.RetentionEvent{ID: id(), Tenant: c.Tenant, Scope: r.Bindings[0].Scope, Kind: "dispose_snapshot", TargetID: r.SnapshotID, Caller: c, Command: r.Command, SnapshotDisposal: &r, AdditionalAuthorization: append([]model.AuthorizationDecision(nil), receipts...), Limitations: []string{"structural_metadata_retained", "source_facts_retained", "backups_expire_under_separate_policy", "archives_expire_under_separate_policy"}}
		out.ID = event.ID
		return retention.StageRetention(ctx, event)
	}, mutationCheck{onReceipt: check, onCommit: check})
}
