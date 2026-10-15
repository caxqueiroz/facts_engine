package model

import "encoding/hex"

// AttestationTombstone retains exact version/evidence binding and provenance
// while omitting confirmation presentation details and delegated proof content.
func AttestationTombstone(a Attestation) Attestation {
	if !a.PayloadDisposed {
		a.DisposedPayloadDigest = Digest(a)
	}
	a.PayloadDisposed = true
	a.ReviewAuthorization = nil
	a.EvidenceAuthorization = nil
	a.Delegation = nil
	if a.Confirmation != nil {
		a.Confirmation = &ClientConfirmation{Presentation: a.Confirmation.Presentation, Response: a.Confirmation.Response}
	}
	return a
}

func ValidAttestationTombstone(a Attestation, tenant, id string) bool {
	digest, err := hex.DecodeString(a.DisposedPayloadDigest)
	return err == nil && len(digest) == 32 && a.DisposedPayloadDigest == hex.EncodeToString(digest) && a.PayloadDisposed && a.Attestor.Tenant == tenant && a.ID == id && Digest(a) == Digest(AttestationTombstone(a))
}
