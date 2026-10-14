package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

func validConfirmation(a model.Attestation) bool {
	if a.Kind != "client_confirmation" {
		return a.Confirmation == nil
	}
	v := a.Confirmation
	return v != nil && (a.ExpiresAt == nil || a.ExpiresAt.After(v.RespondedAt)) && !a.Attestor.Service && a.Attestor.Initiator == "" && model.ValidID(v.PresentationVersion) && model.ValidID(v.Presentation.EvidenceID) && model.ValidID(v.Response.EvidenceID) && v.Presentation.EvidenceID != v.Response.EvidenceID && a.EvidenceID == v.Response.EvidenceID && validDigest(v.Presentation.Digest) && validDigest(v.Response.Digest) && !v.PresentedAt.IsZero() && !v.RespondedAt.IsZero() && !v.RespondedAt.Before(v.PresentedAt) && v.PresentedAt.Equal(v.PresentedAt.Truncate(time.Microsecond)) && v.RespondedAt.Equal(v.RespondedAt.Truncate(time.Microsecond))
}
func (s *Service) verifyAttestationEvidence(ctx context.Context, c model.Caller, a model.Attestation) error {
	data, err := s.loadEvidence(ctx, c, a.Scope, model.AttestationEvidence(a))
	if err != nil {
		return err
	}
	if a.Confirmation != nil {
		for _, ref := range []model.ArtifactReference{a.Confirmation.Presentation, a.Confirmation.Response} {
			if model.BytesDigest(data[ref.EvidenceID]) != ref.Digest {
				return model.ErrPrecondition
			}
		}
	}
	return nil
}

// Verify the candidate prerequisites before taking record/tenant append locks.
// The transaction must select only these immutable IDs; a concurrent new
// prerequisite may require a retry instead of bypassing byte verification.
func (s *Service) verifiedPrerequisites(ctx context.Context, c model.Caller, scope, version, digest, policy string, memory bool) (map[string]bool, error) {
	var as []model.Attestation
	var p model.AcceptancePolicy
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		var err error
		p, err = tx.Policy(ctx, scope, policy)
		if err != nil {
			return err
		}
		if memory {
			t, err := memoryLifecycleTx(tx)
			if err != nil {
				return err
			}
			as, err = t.MemoryAttestations(ctx, version, 0)
			return err
		}
		as, err = tx.Attestations(ctx, version)
		return err
	})
	if err != nil {
		return nil, err
	}
	selected, _ := versionPrerequisites(p, version, digest, as, time.Now())
	verified := map[string]bool{}
	for _, id := range selected {
		for _, a := range as {
			if a.ID == id {
				if err := s.verifyAttestationEvidence(ctx, c, a); err != nil {
					return nil, err
				}
				verified[id] = true
				break
			}
		}
	}
	return verified, nil
}
func requireVerifiedPrerequisites(ids []string, verified map[string]bool) error {
	for _, id := range ids {
		if !verified[id] {
			return model.ErrPrecondition
		}
	}
	return nil
}
