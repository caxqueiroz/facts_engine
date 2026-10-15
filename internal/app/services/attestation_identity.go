package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

func configureAttestationIdentity(s *Service, o model.AttestationIdentityOptions) error {
	if o.Verifier == nil {
		if o.Authority != "" || o.Timeout != 0 || o.MaxAuthenticationAge != 0 {
			return model.ErrInvalid
		}
		return nil
	}
	if o.Timeout == 0 {
		o.Timeout = 5 * time.Second
	}
	if o.MaxAuthenticationAge == 0 {
		o.MaxAuthenticationAge = 15 * time.Minute
	}
	if !model.ValidID(o.Authority) || o.Timeout < time.Second || o.Timeout > 30*time.Second || o.MaxAuthenticationAge < time.Minute || o.MaxAuthenticationAge > 24*time.Hour {
		return model.ErrInvalid
	}
	s.attestationIdentity = o
	return nil
}
func (s *Service) validIdentityReceipt(r model.AttestationIdentityReceipt, request model.AttestationIdentityRequest, now time.Time) bool {
	confirmation := request.Request.Attestation.Confirmation
	return confirmation != nil && !r.AuthenticatedAt.After(confirmation.RespondedAt) && !confirmation.RespondedAt.After(r.VerifiedAt) && r.ExpiresAt.After(r.VerifiedAt) && r.Authority == s.attestationIdentity.Authority && model.ValidID(r.BindingID) && r.RequestDigest == model.AttestationIdentityDigest(request) && r.Attestor == request.Request.Attestation.Attestor && !r.AuthenticatedAt.IsZero() && !r.AuthenticatedAt.After(r.VerifiedAt) && now.Sub(r.AuthenticatedAt) <= s.attestationIdentity.MaxAuthenticationAge && !r.VerifiedAt.IsZero() && now.Sub(r.VerifiedAt) <= 30*time.Second && !r.VerifiedAt.After(now.Add(time.Second)) && r.ExpiresAt.After(now) && !r.ExpiresAt.After(r.VerifiedAt.Add(5*time.Minute))
}
func (s *Service) RecordDelegatedAttestation(ctx context.Context, c model.Caller, r model.DelegatedAttestationRequest) (model.CommitResult, error) {
	a := r.Attestation
	if !c.Service || !model.ValidID(r.GrantID) || !model.ValidID(r.Key) || a.Kind != "client_confirmation" || !validConfirmation(a) || a.Attestor.Tenant != c.Tenant || !model.ValidID(a.Attestor.Principal) || a.PayloadDisposed || a.DisposedPayloadDigest != "" || a.RecordedAt != nil || a.ReviewAuthorization != nil || a.Recorder != nil || a.Delegation != nil || a.EvidenceAuthorization != nil || (c.Initiator != "" && c.Initiator != a.Attestor.Principal) {
		return model.CommitResult{}, model.ErrInvalid
	}
	if s.attestationIdentity.Verifier == nil {
		return model.CommitResult{}, model.ErrPrecondition
	}
	checks := []model.AuthorizationDecision{}
	for _, binding := range []struct {
		caller     model.Caller
		permission string
	}{{c, "can_record_delegated_attestation"}, {c, "can_attest"}, {c, "can_read_evidence"}, {a.Attestor, "can_attest"}, {a.Attestor, "can_read_evidence"}} {
		check, err := s.authorize(ctx, binding.caller, a.Scope, binding.permission, r.Command)
		if err != nil {
			return model.CommitResult{}, err
		}
		checks = append(checks, check)
	}
	request := model.AttestationIdentityRequest{Recorder: c, Request: r}
	bounded, cancel := context.WithTimeout(ctx, s.attestationIdentity.Timeout)
	receipt, err := s.attestationIdentity.Verifier.VerifyAttestationIdentity(bounded, request)
	timedOut := bounded.Err() != nil
	cancel()
	if !timedOut && errors.Is(err, model.ErrDenied) {
		return model.CommitResult{}, model.ErrDenied
	}
	if err != nil || timedOut {
		return model.CommitResult{}, model.ErrUnavailable
	}
	if !s.validIdentityReceipt(receipt, request, time.Now()) {
		return model.CommitResult{}, model.ErrDenied
	}
	delegation := &model.AttestationDelegation{Receipt: receipt, Authorization: checks}
	return s.recordAttestation(ctx, c, r.AttestationRequest, r, delegation)
}

// Run again after Append, inside the transaction: waiting for the tenant revision
// lock must not let an expired identity grant commit.
func freshAttestationIdentity(a model.Attestation, now time.Time) error {
	if a.ReviewAuthorization != nil && now.Sub(a.ReviewAuthorization.CheckedAt) > 30*time.Second {
		return model.ErrUnavailable
	}
	if a.EvidenceAuthorization != nil && now.Sub(a.EvidenceAuthorization.CheckedAt) > 30*time.Second {
		return model.ErrUnavailable
	}
	if a.Delegation == nil {
		return nil
	}
	r := a.Delegation.Receipt
	if !r.ExpiresAt.After(now) || now.Sub(r.VerifiedAt) > 30*time.Second {
		return model.ErrUnavailable
	}
	for _, check := range a.Delegation.Authorization {
		if now.Sub(check.CheckedAt) > 30*time.Second {
			return model.ErrUnavailable
		}
	}
	return nil
}
