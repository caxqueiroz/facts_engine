package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
)

func retentionTx(tx model.Tx) (model.RetentionTx, error) {
	t, ok := tx.(model.RetentionTx)
	if !ok {
		return nil, model.ErrPrecondition
	}
	return t, nil
}
func (s *Service) retentionMutation(ctx context.Context, c model.Caller, scope, kind, permission, target string, cmd model.Command, input any, event model.RetentionEvent, dispose bool) (model.CommitResult, error) {
	return s.mutate(ctx, c, scope, kind, permission, cmd, input, func(tx model.Tx, out *model.CommitResult) error {
		t, err := retentionTx(tx)
		if err != nil {
			return err
		}
		event.ID = id()
		event.Tenant = c.Tenant
		event.Scope = scope
		event.Kind = kind
		event.TargetID = target
		event.Caller = c
		event.Command = cmd
		if dispose {
			if len(event.Disposal.DerivativeEvidenceIDs) > 0 {
				lineage, ok := tx.(model.EvidenceLineageDisposalTx)
				if !ok {
					return model.ErrPrecondition
				}
				err = lineage.DisposeEvidenceLineage(ctx, *event.Disposal)
			} else {
				err = t.DisposeEvidence(ctx, *event.Disposal)
			}
			if err != nil {
				return err
			}
		}
		if event.OrphanDisposal != nil {
			orphan, ok := tx.(model.OrphanEvidenceDisposalTx)
			if !ok {
				return model.ErrPrecondition
			}
			if err := orphan.DisposeOrphanEvidence(ctx, *event.OrphanDisposal); err != nil {
				return err
			}
		}
		out.ID = event.ID
		return t.StageRetention(ctx, event)
	})
}
func (s *Service) RegisterRetentionPolicy(ctx context.Context, c model.Caller, r model.RetentionPolicyRequest) (model.CommitResult, error) {
	p := r.Policy
	if !model.ValidID(p.ID) || !model.ValidID(p.Scope) || (p.RecordClass != "evidence_bytes" && p.RecordClass != "orphan_evidence_bytes" && p.RecordClass != "snapshot_payload" && p.RecordClass != "context_payload" && p.RecordClass != "calculation_payload" && p.RecordClass != "decision_payload" && p.RecordClass != "extraction_job_payload" && p.RecordClass != "extraction_request_payload" && p.RecordClass != "extraction_response_payload" && p.RecordClass != "fact_payload" && p.RecordClass != "memory_payload" && p.RecordClass != "attestation_payload") || p.Purpose == "" || len(p.Purpose) > 256 || !model.ValidID(p.Jurisdiction) || p.MinimumAgeSeconds < 0 || p.MinimumAgeSeconds > 100*366*86400 {
		return model.CommitResult{}, model.ErrInvalid
	}
	return s.retentionMutation(ctx, c, p.Scope, "register_retention_policy", "can_manage_retention", p.ID, r.Command, r, model.RetentionEvent{Policy: &p}, false)
}
func (s *Service) PlaceLegalHold(ctx context.Context, c model.Caller, r model.LegalHoldRequest) (model.CommitResult, error) {
	if !model.ValidID(r.ID) || r.Reason == "" || len(r.Reason) > 1024 {
		return model.CommitResult{}, model.ErrInvalid
	}
	return s.retentionMutation(ctx, c, r.Scope, "place_legal_hold", "can_manage_legal_hold", r.ID, r.Command, r, model.RetentionEvent{Hold: &r}, false)
}
func (s *Service) ReleaseLegalHold(ctx context.Context, c model.Caller, r model.ReleaseLegalHoldRequest) (model.CommitResult, error) {
	if !model.ValidID(r.ID) || r.Reason == "" || len(r.Reason) > 1024 {
		return model.CommitResult{}, model.ErrInvalid
	}
	return s.retentionMutation(ctx, c, r.Scope, "release_legal_hold", "can_manage_legal_hold", r.ID, r.Command, r, model.RetentionEvent{Release: &r}, false)
}
func (s *Service) DisposeEvidence(ctx context.Context, c model.Caller, r model.EvidenceDisposalRequest) (model.CommitResult, error) {
	if !model.ValidID(r.EvidenceID) || !model.ValidID(r.PolicyID) || !model.ValidID(r.Jurisdiction) || r.Reason == "" || len(r.Reason) > 1024 {
		return model.CommitResult{}, model.ErrInvalid
	}
	if len(r.DerivativeEvidenceIDs) > 256 {
		return model.CommitResult{}, model.ErrInvalid
	}
	seen := map[string]bool{r.EvidenceID: true}
	for _, id := range r.DerivativeEvidenceIDs {
		if !model.ValidID(id) || seen[id] {
			return model.CommitResult{}, model.ErrInvalid
		}
		seen[id] = true
	}
	r.DerivativeEvidenceIDs = append([]string(nil), r.DerivativeEvidenceIDs...)
	if _, err := s.authorize(ctx, c, r.Scope, "can_dispose", r.Command); err != nil {
		return model.CommitResult{}, err
	}
	if err := s.checkDisposalDomain(ctx); err != nil {
		return model.CommitResult{}, err
	}

	return s.retentionMutation(ctx, c, r.Scope, "dispose_evidence", "can_dispose", r.EvidenceID, r.Command, r, model.RetentionEvent{Disposal: &r, Limitations: []string{"structural_metadata_retained", "backups_expire_under_separate_policy", "archives_expire_under_separate_policy", "referenced_payloads_require_coordinated_disposal"}}, true)
}
func (s *Service) GetRetentionStatus(ctx context.Context, c model.Caller, q model.RetentionStatusQuery) (model.RetentionStatus, error) {
	if !model.ValidID(q.EvidenceID) {
		return model.RetentionStatus{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, q.Scope, "can_manage_retention", model.Command{Purpose: q.Purpose}); err != nil {
		return model.RetentionStatus{}, err
	}
	var status model.RetentionStatus
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := retentionTx(tx)
		if err != nil {
			return err
		}
		status, err = t.RetentionStatus(ctx, q.Scope, q.EvidenceID)
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		err = model.ErrDenied
	}
	if err != nil {
		return model.RetentionStatus{}, err
	}
	return status, nil
}

func (s *Service) checkDisposalDomain(ctx context.Context) error {
	// The initial disposal adapter operates only on PostgreSQL evidence in the
	// very same retention domain as the ledger. External stores fail closed.
	ledger, ok := s.ledger.(model.RetentionDomain)
	if !ok {
		return model.ErrPrecondition
	}
	store, ok := s.evidence.(model.RetentionDomain)
	if !ok {
		return model.ErrPrecondition
	}
	a, err := ledger.RetentionDomain(ctx)
	if err != nil {
		return err
	}
	b, err := store.RetentionDomain(ctx)
	if err != nil {
		return err
	}
	if a == "" || a != b {
		return model.ErrPrecondition
	}
	return nil
}
func (s *Service) DisposeOrphanEvidence(ctx context.Context, c model.Caller, r model.OrphanEvidenceDisposalRequest) (model.CommitResult, error) {
	if !model.ValidID(r.Object.Locator) || !model.ValidID(r.PolicyID) || !model.ValidID(r.Jurisdiction) || len(r.Object.Version) != 64 || r.Object.Digest != r.Object.Version || r.Object.Size < 0 || r.Object.Size > 16<<20 || r.Reason == "" || len(r.Reason) > 1024 {
		return model.CommitResult{}, model.ErrInvalid
	}
	for _, char := range r.Object.Version {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return model.CommitResult{}, model.ErrInvalid
		}
	}
	if _, err := s.authorize(ctx, c, r.Scope, "can_dispose", r.Command); err != nil {
		return model.CommitResult{}, err
	}
	if err := s.checkDisposalDomain(ctx); err != nil {
		return model.CommitResult{}, err
	}
	return s.retentionMutation(ctx, c, r.Scope, "dispose_orphan_evidence", "can_dispose", r.Object.Locator, r.Command, r, model.RetentionEvent{OrphanDisposal: &r, Limitations: []string{"structural_metadata_retained", "backups_expire_under_separate_policy", "archives_expire_under_separate_policy"}}, false)
}

func (s *Service) ScanOrphanEvidence(ctx context.Context, c model.Caller, q model.OrphanEvidenceQuery) ([]model.ObjectRef, error) {
	if !model.ValidID(q.PolicyID) || !model.ValidID(q.Jurisdiction) || q.Limit < 1 || q.Limit > 100 || (q.AfterLocator != "" && !model.ValidID(q.AfterLocator)) || ((q.AfterLocator == "") != (q.AfterVersion == "")) || len(q.AfterVersion) > 64 {
		return nil, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, q.Scope, "can_dispose", model.Command{Purpose: q.Purpose}); err != nil {
		return nil, err
	}
	var out []model.ObjectRef
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		scanner, ok := tx.(model.OrphanEvidenceScannerTx)
		if !ok {
			return model.ErrPrecondition
		}
		var err error
		out, err = scanner.ScanOrphanEvidence(ctx, q)
		return err
	})
	if err != nil {
		return nil, err
	}
	if _, err := s.authorize(ctx, c, q.Scope, "can_dispose", model.Command{Purpose: q.Purpose}); err != nil {
		return nil, err
	}
	return out, nil
}
