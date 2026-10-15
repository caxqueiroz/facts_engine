package model

import "slices"

// FactDigest is the canonical content digest used by fact proposals. Command
// metadata and disposal state do not participate in the content identity.
func FactDigest(p FactProposal) string {
	return Digest(struct {
		Key     FactKey
		Value   Value
		Valid   Interval
		Schema  string
		Sources []SourceSpan
	}{p.Key, p.Value, p.Valid, p.SchemaVersion, p.Sources})
}

// VersionTombstone removes the typed value and source quotes, preserving the
// original digest, identities, locators and governance provenance. This is not
// disposal authorization, and the metadata cannot reconstruct original content.
// It does not mutate the input.
func VersionTombstone(v Version) Version {
	v.PayloadDisposed = true
	v.Proposal.Value = Value{}
	v.Proposal.Sources = sourceTombstones(v.Proposal.Sources)
	return v
}

// MemoryVersionTombstone removes the body and source quotes while preserving
// dependency identities and their assertion/uncertainty labels. As with
// VersionTombstone, the original digest is provenance, not a digest of metadata.
func MemoryVersionTombstone(v MemoryVersion) MemoryVersion {
	v.PayloadDisposed = true
	v.Proposal.Body = ""
	v.Proposal.Sources = sourceTombstones(v.Proposal.Sources)
	return v
}

func sourceTombstones(spans []SourceSpan) []SourceSpan {
	spans = slices.Clone(spans)
	for i := range spans {
		spans[i].Quote = ""
	}
	return spans
}
