package model

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestContextTombstoneContentAndProvenance(t *testing.T) {
	text := "PRIVATE-fact"
	original := ContextManifest{ID: "context", Tenant: "tenant", Scope: "scope", CanonicalVersion: "context-manifest-v2", Request: ContextRequest{IncludeMemories: true, Query: RecallRequest{Scopes: []string{"scope"}, Query: "PRIVATE-query"}}, Items: []RecallItem{{Key: FactKey{Scope: "scope"}, Fact: FactResult{RecordID: "fact", VersionID: "fact-v1", Value: &Value{Kind: Text, Text: &text}}}}, Memories: []MemoryRecallItem{{Key: MemoryKey{Scope: "memory-scope", ID: "memory"}, Memory: MemoryResult{RecordID: "memory", VersionID: "memory-v1", Body: "PRIVATE-memory", Dependencies: []MemoryDependency{{Scope: "dependency-scope", RecordID: "dependency", VersionID: "dep-v1", ContentDigest: "digest", Label: "PRIVATE-label"}}}}}, Rendered: []byte("PRIVATE-rendered"), QueryReference: "query-digest", SelectionRevision: 12, CreationRevision: 13, Order: []VersionReference{{RecordID: "memory", VersionID: "memory-v1"}}}
	original.RenderedDigest = BytesDigest(original.Rendered)
	original.Digest = ContextDigest(original)
	before, _ := json.Marshal(original)
	tombstone := ContextTombstone(original)
	after, _ := json.Marshal(original)
	if !bytes.Equal(before, after) {
		t.Fatal("input mutated")
	}
	raw, _ := json.Marshal(tombstone)
	if bytes.Contains(raw, []byte("PRIVATE-")) || len(tombstone.Rendered) != 0 || tombstone.Items[0].Fact.Value != nil {
		t.Fatal("content retained")
	}
	if !tombstone.PayloadDisposed || tombstone.Digest != original.Digest || tombstone.RenderedDigest != original.RenderedDigest || tombstone.QueryReference != original.QueryReference || tombstone.SelectionRevision != 12 || tombstone.CreationRevision != 13 {
		t.Fatal("original identity lost")
	}
	if !reflect.DeepEqual(ContextScopes(tombstone), []string{"dependency-scope", "memory-scope", "scope"}) || !reflect.DeepEqual(tombstone.Order, original.Order) {
		t.Fatal("authorization or selection inventory lost")
	}
	dependency := tombstone.Memories[0].Memory.Dependencies[0]
	if dependency.RecordID != "dependency" || dependency.VersionID != "dep-v1" || dependency.ContentDigest != "digest" || dependency.Label != "" {
		t.Fatal("dependency provenance changed")
	}
	if !reflect.DeepEqual(ContextTombstone(tombstone), tombstone) {
		t.Fatal("tombstone not idempotent")
	}
	if ContextDigest(tombstone) == original.Digest {
		t.Fatal("metadata claimed original content identity")
	}
	marked := original
	marked.PayloadDisposed = true
	if ContextDigest(marked) != original.Digest {
		t.Fatal("disposal state changed content digest")
	}
	if bytes.Contains(before, []byte("PayloadDisposed")) {
		t.Fatal("legacy serialization changed")
	}
}
