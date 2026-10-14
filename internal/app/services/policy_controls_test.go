package services

import (
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

func TestAttestationAgeBoundaries(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	expiry := at.Add(time.Hour)
	a := model.Attestation{RecordedAt: &at, ExpiresAt: &expiry}
	req := model.AttestationRequirement{RequireExpiry: true, MaxAgeSeconds: 60}
	if !attestationEligible(req, a, at.Add(59*time.Second)) {
		t.Fatal("fresh attestation rejected")
	}
	if attestationEligible(req, a, at.Add(time.Minute)) {
		t.Fatal("maximum age boundary ignored")
	}
	if attestationEligible(req, a, at.Add(-time.Second)) {
		t.Fatal("future recording eligible")
	}
	a.RecordedAt = nil
	if attestationEligible(req, a, at) {
		t.Fatal("fabricated legacy age")
	}
	a.RecordedAt = &at
	a.ExpiresAt = nil
	if attestationEligible(req, a, at) {
		t.Fatal("mandatory expiry omitted")
	}
	req.RequireExpiry = false
	if !attestationEligible(req, a, at) {
		t.Fatal("implicit age limit unavailable")
	}
	a.ExpiresAt = &at
	if attestationEligible(req, a, at) {
		t.Fatal("explicit expiry boundary ignored")
	}
}
func TestPolicyControls(t *testing.T) {
	p := model.AcceptancePolicy{ID: "policy", Mode: "automated", AcceptingServices: []string{"service"}, SourceClasses: []string{"customer"}, Required: []model.AttestationRequirement{{Kind: "human_review", Principals: []string{"reviewer"}, RequireExpiry: true, MaxAgeSeconds: 60}}}
	if err := validatePolicy(p); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*model.AcceptancePolicy){func(p *model.AcceptancePolicy) { p.Required[0].MaxAgeSeconds = -1 }, func(p *model.AcceptancePolicy) { p.Required[0].MaxAgeSeconds = 366 * 24 * 60 * 60 }, func(p *model.AcceptancePolicy) { p.SourceClasses = []string{"system"} }, func(p *model.AcceptancePolicy) { p.SourceClasses = []string{"assistant"} }, func(p *model.AcceptancePolicy) { p.Required[0].Principals = []string{"reviewer", "reviewer"} }, func(p *model.AcceptancePolicy) { p.AcceptingServices = []string{"service", "service"} }} {
		copy := p
		copy.Required = append([]model.AttestationRequirement(nil), p.Required...)
		change(&copy)
		if validatePolicy(copy) == nil {
			t.Fatal("invalid policy accepted", copy)
		}
	}
	expiry := time.Now().Add(time.Hour)
	a := model.Attestation{Kind: "human_review", Attestor: model.Caller{Tenant: "tenant", Principal: "reviewer"}, ExpiresAt: &expiry}
	if attestationPolicyChecks(p, a, a.Attestor) == nil {
		t.Fatal("self review")
	}
	if attestationPolicyChecks(p, a, model.Caller{Principal: "service", Service: true, Initiator: "reviewer"}) == nil {
		t.Fatal("initiating proposer reviewed itself")
	}
	if err := attestationPolicyChecks(p, a, model.Caller{Principal: "proposer"}); err != nil {
		t.Fatal(err)
	}
	a.Attestor.Service = true
	if attestationPolicyChecks(p, a, model.Caller{Principal: "proposer"}) == nil {
		t.Fatal("automated human review")
	}
}
