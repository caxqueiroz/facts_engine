package model

import (
	"context"
	"time"
)

// GrantID is an opaque reference, never a bearer token or raw identity assertion.
type DelegatedAttestationRequest struct {
	AttestationRequest
	GrantID string
}

// The verifier must authenticate the grant and bind ALL request fields, including
// actor, tenant, scope, command, exact version/digest, artifacts and timestamps.
// Returning the request's claimed identity without independent verification is unsafe.
type AttestationIdentityRequest struct {
	Recorder Caller
	Request  DelegatedAttestationRequest
}
type AttestationIdentityVerifier interface {
	VerifyAttestationIdentity(context.Context, AttestationIdentityRequest) (AttestationIdentityReceipt, error)
}
type AttestationIdentityReceipt struct {
	Authority, BindingID, RequestDigest    string
	Attestor                               Caller
	AuthenticatedAt, VerifiedAt, ExpiresAt time.Time
}
type AttestationIdentityOptions struct {
	Verifier                      AttestationIdentityVerifier
	Authority                     string
	Timeout, MaxAuthenticationAge time.Duration
}
type AttestationDelegation struct {
	Receipt       AttestationIdentityReceipt
	Authorization []AuthorizationDecision
}

func AttestationIdentityDigest(r AttestationIdentityRequest) string { return Digest(r) }
