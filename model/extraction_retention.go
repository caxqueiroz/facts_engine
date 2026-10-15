package model

import "slices"

// ExtractionJobTombstone removes copied content from a scheduling artifact while
// preserving IDs, scopes, profile/schema bindings and original digests. It does
// not authorize disposal or validate the input. The input is not mutated.
func ExtractionJobTombstone(j ExtractionJob) ExtractionJob {
	j.PayloadDisposed = true
	j.Profile.Prompt = nil
	j.Profile.Parameters = nil
	j.Predicates = slices.Clone(j.Predicates)
	for i := range j.Predicates {
		j.Predicates[i].Constraints = nil
	}
	j.DependencyVersions = slices.Clone(j.DependencyVersions)
	for i := range j.DependencyVersions {
		j.DependencyVersions[i] = VersionTombstone(j.DependencyVersions[i])
	}
	return j
}

// ExtractionAttemptTombstone removes the provider request. Original request
// digests, admission reservations, authorization and attempt identity remain.
func ExtractionAttemptTombstone(a ExtractionAttempt) ExtractionAttempt {
	a.PayloadDisposed = true
	a.RequestBytes = nil
	return a
}

// ExtractionResultTombstone removes retained response bytes, including truncated
// responses. ResponseComplete describes the original capture; PayloadDisposed
// states that those bytes are no longer available. Proposals and issue codes
// remain structural provenance. Digests are not anonymization.
func ExtractionResultTombstone(r ExtractionResult) ExtractionResult {
	r.PayloadDisposed = true
	r.ResponseBytes = nil
	return r
}
