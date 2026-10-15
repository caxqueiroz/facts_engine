package model

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestVersionTombstones(t *testing.T) {
	text := "PRIVATE-value"
	fact := Version{ID: "version", RecordID: "record", PolicyID: "policy", ExtractionAttemptID: "attempt", Proposer: Caller{Tenant: "tenant", Principal: "proposer"}, Proposal: FactProposal{Key: FactKey{Scope: "scope", Subject: "subject", Predicate: "predicate"}, SchemaVersion: "v1", Value: Value{Kind: Text, Text: &text}, Sources: []SourceSpan{{EvidenceID: "source", Start: 1, End: 12, Quote: "PRIVATE-quote"}}}}
	// This is the pre-existing canonical layout, independent of the new helper.
	legacy := Digest(struct {
		Key     FactKey
		Value   Value
		Valid   Interval
		Schema  string
		Sources []SourceSpan
	}{fact.Proposal.Key, fact.Proposal.Value, fact.Proposal.Valid, fact.Proposal.SchemaVersion, fact.Proposal.Sources})
	fact.Digest = FactDigest(fact.Proposal)
	if fact.Digest != legacy {
		t.Fatal("legacy digest changed")
	}
	memory := MemoryVersion{ID: "memory", RecordID: "memory-record", PolicyID: "policy", LedgerRevision: 4, Proposer: fact.Proposer, Proposal: MemoryProposal{Key: MemoryKey{Scope: "scope", ID: "summary"}, Subtype: "episode", SchemaVersion: "v1", Body: "PRIVATE-body", Sources: fact.Proposal.Sources, Dependencies: []MemoryDependency{{Scope: "dependency-scope", RecordID: fact.RecordID, VersionID: fact.ID, ContentDigest: fact.Digest, Label: "uncertain"}}}}
	memory.Digest = MemoryDigest(memory.Proposal)
	factBefore, _ := json.Marshal(fact)
	memoryBefore, _ := json.Marshal(memory)
	f, m := VersionTombstone(fact), MemoryVersionTombstone(memory)
	factAfter, _ := json.Marshal(fact)
	memoryAfter, _ := json.Marshal(memory)
	if !bytes.Equal(factBefore, factAfter) || !bytes.Equal(memoryBefore, memoryAfter) {
		t.Fatal("input mutated")
	}
	for _, raw := range [][]byte{factBefore, memoryBefore} {
		if bytes.Contains(raw, []byte("PayloadDisposed")) {
			t.Fatal("legacy serialization changed")
		}
	}
	for _, v := range []any{f, m} {
		raw, _ := json.Marshal(v)
		if bytes.Contains(raw, []byte("PRIVATE-")) {
			t.Fatal("content retained")
		}
	}
	if !f.PayloadDisposed || !m.PayloadDisposed || f.Digest != fact.Digest || m.Digest != memory.Digest || f.ExtractionAttemptID != fact.ExtractionAttemptID || m.LedgerRevision != 4 || !reflect.DeepEqual(m.Proposal.Dependencies, memory.Proposal.Dependencies) {
		t.Fatal("provenance lost")
	}
	if f.Proposal.Sources[0].EvidenceID != "source" || f.Proposal.Sources[0].Start != 1 || f.Proposal.Sources[0].End != 12 {
		t.Fatal("locator lost")
	}
	if FactDigest(f.Proposal) == fact.Digest || MemoryDigest(m.Proposal) == memory.Digest {
		t.Fatal("metadata claimed original content")
	}
	if !reflect.DeepEqual(VersionTombstone(f), f) || !reflect.DeepEqual(MemoryVersionTombstone(m), m) {
		t.Fatal("not idempotent")
	}
}
