package services

import (
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestDisposedAttestationCannotSatisfyAcceptance(t *testing.T) {
	policy := model.AcceptancePolicy{Required: []model.AttestationRequirement{{Kind: "source_verified", Principals: []string{"reviewer"}}}}
	attestation := model.Attestation{ID: "proof", VersionID: "version", ContentDigest: "digest", Kind: "source_verified", Attestor: model.Caller{Tenant: "tenant", Principal: "reviewer"}}
	if ids, err := versionPrerequisites(policy, "version", "digest", []model.Attestation{attestation}, time.Now()); err != nil || len(ids) != 1 || ids[0] != attestation.ID {
		t.Fatal(ids, err)
	}
	disposed := model.AttestationTombstone(attestation)
	if _, err := versionPrerequisites(policy, "version", "digest", []model.Attestation{disposed}, time.Now()); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("disposed proof satisfied a required prerequisite", err)
	}
}
