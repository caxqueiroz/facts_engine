package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) authorizedEvidence(ctx context.Context, c model.Caller, q model.EvidenceQuery, permission string) (model.Evidence, error) {
	if !model.ValidID(q.ID) {
		return model.Evidence{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, q.Scope, permission, model.Command{Purpose: q.Purpose}); err != nil {
		return model.Evidence{}, err
	}
	var e model.Evidence
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error { var err error; e, err = tx.Evidence(ctx, q.ID); return err })
	if errors.Is(err, model.ErrNotFound) {
		return model.Evidence{}, model.ErrDenied
	}
	if err != nil {
		return model.Evidence{}, err
	}
	if e.Scope != q.Scope {
		return model.Evidence{}, model.ErrDenied
	}
	return e, nil
}
func (s *Service) GetEvidenceMetadata(ctx context.Context, c model.Caller, q model.EvidenceQuery) (model.Evidence, error) {
	e, err := s.authorizedEvidence(ctx, c, q, "can_read_evidence_metadata")
	if err != nil {
		return model.Evidence{}, err
	}
	if _, err = s.authorize(ctx, c, q.Scope, "can_read_evidence_metadata", model.Command{Purpose: q.Purpose}); err != nil {
		return model.Evidence{}, err
	}
	return e, nil
}
func (s *Service) GetEvidenceContent(ctx context.Context, c model.Caller, q model.EvidenceQuery) (model.EvidenceContent, error) {
	e, err := s.authorizedEvidence(ctx, c, q, "can_read_evidence")
	if err != nil {
		return model.EvidenceContent{}, err
	}
	b, err := s.verify(ctx, c.Tenant, e)
	if err != nil {
		return model.EvidenceContent{}, err
	}
	if _, err = s.authorize(ctx, c, q.Scope, "can_read_evidence", model.Command{Purpose: q.Purpose}); err != nil {
		return model.EvidenceContent{}, err
	}
	return model.EvidenceContent{Metadata: e, Bytes: b}, nil
}
