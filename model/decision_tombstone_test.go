package model

import (
	"testing"
	"time"
)

func TestDecisionTombstone(t *testing.T) {
	r := DecisionRecord{ID: "decision", Tenant: "tenant", Scopes: []string{"scope"}, Digest: "original-digest", SourceDigest: "source-digest", Receipt: DecisionReceipt{Scope: "scope", ApplicationID: "app", ExternalDecisionID: "external", ContextID: "context", SelectedItems: []VersionReference{{RecordID: "record", VersionID: "version"}}, UsedItems: []VersionReference{{RecordID: "record", VersionID: "version"}}, MethodKind: "model", MethodID: "private-method", MethodVersion: "private-version", TransformationVersion: "private-transform", DecidedAt: time.Now(), Outcome: "private-outcome", Input: ArtifactReference{EvidenceID: "input", Digest: "input-digest"}, Output: ArtifactReference{EvidenceID: "output", Digest: "output-digest"}, ModelRequest: &ArtifactReference{EvidenceID: "request", Digest: "request-digest"}}}
	tombstone := DecisionTombstone(r)
	if !ValidDecisionTombstone(tombstone, "tenant", "decision") || tombstone.Digest != r.Digest || tombstone.SourceDigest != r.SourceDigest || tombstone.Receipt.ContextID != r.Receipt.ContextID || Digest(tombstone.Receipt.SelectedItems) != Digest(r.Receipt.SelectedItems) || Digest(DecisionArtifacts(tombstone.Receipt)) != Digest(DecisionArtifacts(r.Receipt)) {
		t.Fatal(tombstone)
	}
	if r.PayloadDisposed || r.Receipt.MethodID == "" {
		t.Fatal("source mutated")
	}
	for _, change := range []func(*DecisionRecord){
		func(r *DecisionRecord) { r.Receipt.MethodKind = "secret" },
		func(r *DecisionRecord) { r.Receipt.TransformationVersion = "secret" },
		func(r *DecisionRecord) { r.Receipt.DecidedAt = time.Now() },
		func(r *DecisionRecord) { r.Receipt.Outcome = "secret" },
		func(r *DecisionRecord) { r.PayloadDisposed = false },
		func(r *DecisionRecord) { r.ID = "other" },
		func(r *DecisionRecord) { r.Tenant = "other" },
	} {
		changed := DecisionTombstone(r)
		change(&changed)
		if ValidDecisionTombstone(changed, "tenant", "decision") {
			t.Fatal("invalid tombstone accepted", changed)
		}
	}
}
