package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"sort"
)

func (s *Service) DisposeVersion(ctx context.Context, c model.Caller, r model.VersionDisposalRequest) (model.CommitResult, error) {
	if (r.Kind != "fact" && r.Kind != "memory") || !model.ValidID(r.Scope) || !model.ValidID(r.RecordID) || !model.ValidID(r.VersionID) || r.Reason == "" || len(r.Reason) > 1024 || len(r.Bindings) < 1 || len(r.Bindings) > 256 {
		return model.CommitResult{}, model.ErrInvalid
	}
	r.Bindings = append([]model.VersionRetentionBinding(nil), r.Bindings...)
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
	return s.mutate(ctx, c, r.Scope, "dispose_"+r.Kind+"_payload", "can_dispose", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		disposal, ok := tx.(model.VersionDisposalTx)
		if !ok {
			return model.ErrPrecondition
		}
		retention, err := retentionTx(tx)
		if err != nil {
			return err
		}
		revision, err := disposal.DisposeVersion(ctx, r)
		if err != nil {
			return err
		}
		if err := check(); err != nil {
			return err
		}
		event := model.RetentionEvent{ID: id(), Tenant: c.Tenant, Scope: r.Scope, Kind: "dispose_" + r.Kind + "_payload", TargetID: r.VersionID, Caller: c, Command: r.Command, VersionDisposal: &r, AdditionalAuthorization: append([]model.AuthorizationDecision(nil), receipts...), Limitations: []string{"structural_metadata_retained", "projection_cleanup_requires_acknowledgements", "source_evidence_retained", "backups_expire_under_separate_policy", "archives_expire_under_separate_policy"}}
		out.ID = event.ID
		out.RecordKind, out.RecordID, out.VersionID, out.StreamRevision = r.Kind, r.RecordID, r.VersionID, revision
		return retention.StageRetention(ctx, event)
	}, mutationCheck{onReceipt: check, onCommit: check})
}
