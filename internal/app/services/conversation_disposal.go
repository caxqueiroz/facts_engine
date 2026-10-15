package services

import (
	"context"

	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) DisposeConversation(ctx context.Context, c model.Caller, r model.ConversationDisposalRequest) (model.CommitResult, error) {
	if !model.ValidID(r.Scope) || !model.ValidID(r.ConversationID) || !model.ValidID(r.PolicyID) || !model.ValidID(r.Jurisdiction) || r.Purpose == "" || len(r.Purpose) > 256 || r.Reason == "" || len(r.Reason) > 1024 {
		return model.CommitResult{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, r.Scope, "can_dispose", r.Command); err != nil {
		return model.CommitResult{}, err
	}
	return s.mutate(ctx, c, r.Scope, "dispose_conversation", "can_dispose", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		disposal, ok := tx.(model.ConversationDisposalTx)
		if !ok {
			return model.ErrPrecondition
		}
		retention, err := retentionTx(tx)
		if err != nil {
			return err
		}
		if err := disposal.DisposeConversation(ctx, r); err != nil {
			return err
		}
		check, err := s.authorize(ctx, c, r.Scope, "can_dispose", r.Command)
		if err != nil {
			return err
		}
		event := model.RetentionEvent{ID: id(), Tenant: c.Tenant, Scope: r.Scope, Kind: "dispose_conversation", TargetID: r.ConversationID, Caller: c, Command: r.Command, ConversationDisposal: &r, AdditionalAuthorization: []model.AuthorizationDecision{check}, Limitations: []string{"source_evidence_retained_under_separate_policy", "exports_and_archives_expire_under_separate_policy", "backups_expire_under_separate_policy"}}
		out.ID = event.ID
		return retention.StageRetention(ctx, event)
	}, mutationCheck{onReceipt: func() error { _, err := s.authorize(ctx, c, r.Scope, "can_dispose", r.Command); return err }, onCommit: func() error { _, err := s.authorize(ctx, c, r.Scope, "can_dispose", r.Command); return err }})
}
