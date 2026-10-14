package connectapi

import (
	"context"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type AttestationEngine interface {
	RecordAttestation(context.Context, model.Caller, model.AttestationRequest) (model.CommitResult, error)
	RecordDelegatedAttestation(context.Context, model.Caller, model.DelegatedAttestationRequest) (model.CommitResult, error)
	WithdrawAttestation(context.Context, model.Caller, model.Withdrawal) (model.CommitResult, error)
}
type attestationService struct{ engine AttestationEngine }

func attestationFromProto(a *v1.AttestationInput, attestor model.Caller) model.Attestation {
	out := model.Attestation{ID: a.Id, Scope: a.Scope, RecordID: a.RecordId, VersionID: a.VersionId, ContentDigest: a.ContentDigest, Kind: a.Kind, EvidenceID: a.EvidenceId, Attestor: attestor, ExpiresAt: optionalTime(a.ExpiresAt)}
	if a.Confirmation != nil {
		c := a.Confirmation
		out.Confirmation = &model.ClientConfirmation{Presentation: model.ArtifactReference{EvidenceID: c.Presentation.EvidenceId, Digest: c.Presentation.Digest}, Response: model.ArtifactReference{EvidenceID: c.Response.EvidenceId, Digest: c.Response.Digest}, PresentationVersion: c.PresentationVersion, PresentedAt: c.PresentedAt.AsTime(), RespondedAt: c.RespondedAt.AsTime()}
	}
	return out
}
func (s *attestationService) RecordAttestation(ctx context.Context, r *v1.RecordAttestationRequest) (*v1.RecordAttestationResponse, error) {
	caller := callerFrom(ctx)
	out, err := s.engine.RecordAttestation(ctx, caller, model.AttestationRequest{Command: command(r.Command), Attestation: attestationFromProto(r.Attestation, caller)})
	if err != nil {
		return nil, err
	}
	return &v1.RecordAttestationResponse{Result: commit(out)}, nil
}
func (s *attestationService) RecordDelegatedAttestation(ctx context.Context, r *v1.RecordDelegatedAttestationRequest) (*v1.RecordDelegatedAttestationResponse, error) {
	caller := callerFrom(ctx)
	// The engine independently verifies this claimed human against the grant.
	attestor := model.Caller{Tenant: caller.Tenant, Principal: r.AttestorPrincipal}
	out, err := s.engine.RecordDelegatedAttestation(ctx, caller, model.DelegatedAttestationRequest{AttestationRequest: model.AttestationRequest{Command: command(r.Command), Attestation: attestationFromProto(r.Attestation, attestor)}, GrantID: r.GrantId})
	if err != nil {
		return nil, err
	}
	return &v1.RecordDelegatedAttestationResponse{Result: commit(out)}, nil
}
func (s *attestationService) WithdrawAttestation(ctx context.Context, r *v1.WithdrawAttestationRequest) (*v1.WithdrawAttestationResponse, error) {
	out, err := s.engine.WithdrawAttestation(ctx, callerFrom(ctx), model.Withdrawal{Command: command(r.Command), Scope: r.Scope, AttestationID: r.AttestationId})
	if err != nil {
		return nil, err
	}
	return &v1.WithdrawAttestationResponse{Result: commit(out)}, nil
}
