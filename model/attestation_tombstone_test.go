package model

import (
	"testing"
	"time"
)

func TestAttestationTombstone(t *testing.T) {
	a := Attestation{ID: "attestation", Scope: "scope", RecordID: "record", VersionID: "version", ContentDigest: "version-digest", Kind: "client_confirmation", EvidenceID: "response", Attestor: Caller{Tenant: "tenant", Principal: "reviewer"}, ReviewAuthorization: &AuthorizationDecision{}, EvidenceAuthorization: &AuthorizationDecision{}, Delegation: &AttestationDelegation{}, Confirmation: &ClientConfirmation{Presentation: ArtifactReference{EvidenceID: "presentation", Digest: "presentation-digest"}, Response: ArtifactReference{EvidenceID: "response", Digest: "response-digest"}, PresentationVersion: "private-template", PresentedAt: time.Now(), RespondedAt: time.Now()}}
	tombstone := AttestationTombstone(a)
	if !ValidAttestationTombstone(tombstone, "tenant", a.ID) || tombstone.DisposedPayloadDigest != Digest(a) || Digest(AttestationEvidence(tombstone)) != Digest(AttestationEvidence(a)) || tombstone.ContentDigest != a.ContentDigest {
		t.Fatal(tombstone)
	}
	if a.PayloadDisposed || a.Confirmation.PresentationVersion == "" || a.Delegation == nil {
		t.Fatal("source mutated")
	}
	for _, change := range []func(*Attestation){
		func(a *Attestation) { a.Confirmation.PresentationVersion = "secret" },
		func(a *Attestation) { a.Confirmation.RespondedAt = time.Now() },
		func(a *Attestation) { a.Delegation = &AttestationDelegation{} },
		func(a *Attestation) { a.ReviewAuthorization = &AuthorizationDecision{} },
		func(a *Attestation) { a.DisposedPayloadDigest = "invalid" },
		func(a *Attestation) { a.PayloadDisposed = false },
		func(a *Attestation) { a.ID = "other" },
		func(a *Attestation) { a.Attestor.Tenant = "other" },
	} {
		changed := AttestationTombstone(a)
		change(&changed)
		if ValidAttestationTombstone(changed, "tenant", a.ID) {
			t.Fatal("invalid tombstone accepted", changed)
		}
	}
}
