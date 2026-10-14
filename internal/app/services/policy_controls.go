package services

import (
	"github.com/caxqueiroz/facts_engine/model"
	"slices"
	"time"
)

func validatePolicy(p model.AcceptancePolicy) error {
	if !model.ValidID(p.ID) || (p.Mode != "manual" && p.Mode != "automated") || len(p.SourceClasses) == 0 || len(p.SourceClasses) > 64 || len(p.AcceptingServices) > 64 || len(p.Required) > 4 || (p.Mode == "automated" && len(p.AcceptingServices) == 0) {
		return model.ErrInvalid
	}
	for _, list := range [][]string{p.SourceClasses, p.AcceptingServices} {
		seen := map[string]bool{}
		for _, id := range list {
			if !model.ValidID(id) || seen[id] {
				return model.ErrInvalid
			}
			seen[id] = true
		}
	}
	if slices.Contains(p.SourceClasses, "assistant") || slices.Contains(p.SourceClasses, "system") {
		return model.ErrInvalid
	}
	kinds := map[string]bool{}
	for _, req := range p.Required {
		if !validKind(req.Kind) || len(req.Principals) == 0 || len(req.Principals) > 64 || kinds[req.Kind] || req.MaxAgeSeconds < 0 || req.MaxAgeSeconds > 365*24*60*60 {
			return model.ErrInvalid
		}
		kinds[req.Kind] = true
		seen := map[string]bool{}
		for _, id := range req.Principals {
			if !model.ValidID(id) || seen[id] {
				return model.ErrInvalid
			}
			seen[id] = true
		}
	}
	return nil
}
func attestationPolicyChecks(p model.AcceptancePolicy, a model.Attestation, proposer model.Caller) error {
	for _, required := range p.Required {
		if required.Kind != a.Kind || !slices.Contains(required.Principals, a.Attestor.Principal) {
			continue
		}
		if required.RequireExpiry && a.ExpiresAt == nil {
			return model.ErrPrecondition
		}
		if a.Kind == "human_review" && (a.Attestor.Service || a.Attestor.Initiator != "" || a.Attestor.Principal == proposer.Principal || a.Attestor.Human() == proposer.Human()) {
			return model.ErrPrecondition
		}
		return nil
	}
	return model.ErrDenied
}
func attestationEligible(req model.AttestationRequirement, a model.Attestation, now time.Time) bool {
	if req.RequireExpiry && a.ExpiresAt == nil {
		return false
	}
	if a.ExpiresAt != nil && !a.ExpiresAt.After(now) {
		return false
	}
	if req.MaxAgeSeconds > 0 {
		if a.RecordedAt == nil || a.RecordedAt.IsZero() || a.RecordedAt.After(now) || !a.RecordedAt.Add(time.Duration(req.MaxAgeSeconds)*time.Second).After(now) {
			return false
		}
	}
	if a.Kind == "human_review" {
		check := a.ReviewAuthorization
		if a.Attestor.Service || a.Attestor.Initiator != "" || check == nil || !check.Allowed || check.Request.Caller != a.Attestor || check.Request.Scope != a.Scope || check.Request.Permission != "can_approve" {
			return false
		}
	}
	return true
}

// Identity freshness gates both disclosure and writes. Acceptance eligibility
// gates new writes only; a stored receipt must not retroactively re-accept facts.
type mutationCheck struct{ onCommit, onReceipt func() error }

func commitOnlyCheck(check func() error) mutationCheck { return mutationCheck{onCommit: check} }
func freshnessCheck(check func() error) mutationCheck {
	return mutationCheck{onCommit: check, onReceipt: check}
}
