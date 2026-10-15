package model

import (
	"testing"
	"time"
)

func TestCalculationTombstone(t *testing.T) {
	r := CalculationRecord{ID: "calculation", Tenant: "tenant", Digest: "retained-digest", Receipt: CalculationReceipt{Scope: "scope", ApplicationID: "app", ExternalRunID: "run", SnapshotID: "snapshot", ReferenceSnapshotIDs: []string{"reference"}, MethodID: "private-method", MethodVersion: "private-version", ExecutableDigest: "private-executable", ConfigurationDigest: "private-config", NormalizationVersion: "private-normalization", StartedAt: time.Now(), CompletedAt: time.Now(), Outcome: "private-outcome", Input: ArtifactReference{EvidenceID: "input", Digest: "input-digest"}, Output: ArtifactReference{EvidenceID: "output", Digest: "output-digest"}}}
	tombstone := CalculationTombstone(r)
	if !ValidCalculationTombstone(tombstone, "tenant", "calculation") || tombstone.Digest != r.Digest || tombstone.Receipt.Input != r.Receipt.Input || tombstone.Receipt.SnapshotID != r.Receipt.SnapshotID {
		t.Fatal(tombstone)
	}
	if r.PayloadDisposed || r.Receipt.MethodID == "" {
		t.Fatal("source mutated")
	}
	tombstone.Receipt.ReferenceSnapshotIDs[0] = "changed"
	if r.Receipt.ReferenceSnapshotIDs[0] != "reference" {
		t.Fatal("lineage slice shared")
	}
	for _, change := range []func(*CalculationRecord){
		func(r *CalculationRecord) { r.Receipt.MethodID = "secret" },
		func(r *CalculationRecord) { r.Receipt.ConfigurationDigest = "secret" },
		func(r *CalculationRecord) { r.Receipt.StartedAt = time.Now() },
		func(r *CalculationRecord) { r.PayloadDisposed = false },
		func(r *CalculationRecord) { r.ID = "other" },
		func(r *CalculationRecord) { r.Tenant = "other" },
	} {
		changed := CalculationTombstone(r)
		change(&changed)
		if ValidCalculationTombstone(changed, "tenant", "calculation") {
			t.Fatal("invalid tombstone accepted", changed)
		}
	}
}
