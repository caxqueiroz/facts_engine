package model

import "time"

// ClientConfirmation retains the application's presentation and affirmative
// response for the attestation's exact record/version/content digest.
// The trusted attestor binds their meaning; the engine does not interpret PDFs,
// images, or natural language as consent.
type ClientConfirmation struct {
	Presentation, Response   ArtifactReference
	PresentationVersion      string
	PresentedAt, RespondedAt time.Time
}

func AttestationEvidence(a Attestation) []string {
	ids := []string{a.EvidenceID}
	if a.Confirmation != nil && a.Confirmation.Presentation.EvidenceID != a.EvidenceID {
		ids = append(ids, a.Confirmation.Presentation.EvidenceID)
	}
	return ids
}
