package services

import (
	"context"

	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) DisposeAttestation(ctx context.Context, c model.Caller, r model.AttestationDisposalRequest) (model.CommitResult, error) {
	if (r.Kind != "fact" && r.Kind != "memory") || !model.ValidID(r.Scope) || !model.ValidID(r.RecordID) || !model.ValidID(r.VersionID) || !model.ValidID(r.AttestationID) || !model.ValidID(r.PolicyID) || !model.ValidID(r.Jurisdiction) || r.Purpose == "" || len(r.Purpose) > 256 || r.Reason == "" || len(r.Reason) > 1024 {
		return model.CommitResult{}, model.ErrInvalid
	}
	check, err := s.authorize(ctx, c, r.Scope, "can_dispose", r.Command)
	if err != nil {
		return model.CommitResult{}, err
	}
	return s.mutate(ctx, c, r.Scope, "dispose_"+r.Kind+"_attestation", "can_dispose", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		disposal, ok := tx.(model.AttestationDisposalTx)
		if !ok {
			return model.ErrPrecondition
		}
		retention, err := retentionTx(tx)
		if err != nil {
			return err
		}
		revision, err := disposal.DisposeAttestation(ctx, r)
		if err != nil {
			return err
		}
		check, err = s.authorize(ctx, c, r.Scope, "can_dispose", r.Command)
		if err != nil {
			return err
		}
		event := model.RetentionEvent{ID: id(), Tenant: c.Tenant, Scope: r.Scope, Kind: "dispose_" + r.Kind + "_attestation", TargetID: r.AttestationID, Caller: c, Command: r.Command, AttestationDisposal: &r, AdditionalAuthorization: []model.AuthorizationDecision{check}, Limitations: []string{"attestation_identity_retained", "confirmation_evidence_retained", "backups_expire_under_separate_policy", "archives_expire_under_separate_policy"}}
		out.ID = event.ID
		out.RecordKind, out.RecordID, out.VersionID, out.StreamRevision = r.Kind, r.RecordID, r.VersionID, revision
		return retention.StageRetention(ctx, event)
	}, mutationCheck{onReceipt: func() error { _, err := s.authorize(ctx, c, r.Scope, "can_dispose", r.Command); return err }, onCommit: func() error { _, err := s.authorize(ctx, c, r.Scope, "can_dispose", r.Command); return err }})
}
