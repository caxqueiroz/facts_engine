package services

import (
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

func TestConfirmationPrerequisites(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Microsecond)
	base := model.Attestation{ID: "att", Kind: "client_confirmation", EvidenceID: "response", VersionID: "version", ContentDigest: "digest", Attestor: model.Caller{Tenant: "tenant", Principal: "client"}, Confirmation: &model.ClientConfirmation{Presentation: model.ArtifactReference{EvidenceID: "presentation", Digest: model.BytesDigest([]byte("shown"))}, Response: model.ArtifactReference{EvidenceID: "response", Digest: model.BytesDigest([]byte("yes"))}, PresentationVersion: "ui-v1", PresentedAt: at, RespondedAt: at}}
	policy := model.AcceptancePolicy{Required: []model.AttestationRequirement{{Kind: "client_confirmation", Principals: []string{"client"}}}}
	if _, err := versionPrerequisites(policy, "version", "digest", []model.Attestation{base}, at); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*model.Attestation){func(a *model.Attestation) { a.Confirmation = nil }, func(a *model.Attestation) { a.Attestor.Service = true }, func(a *model.Attestation) { a.Confirmation.RespondedAt = at.Add(-time.Second) }, func(a *model.Attestation) { a.EvidenceID = "other" }, func(a *model.Attestation) { a.Confirmation.Presentation.EvidenceID = "response" }, func(a *model.Attestation) { a.Confirmation.Response.Digest = "bad" }} {
		a := base
		copy := *base.Confirmation
		a.Confirmation = &copy
		mutate(&a)
		if validConfirmation(a) {
			t.Fatal("invalid confirmation eligible", a)
		}
		if _, err := versionPrerequisites(policy, "version", "digest", []model.Attestation{a}, at); err == nil {
			t.Fatal("invalid historical confirmation satisfies policy")
		}
	}
}
