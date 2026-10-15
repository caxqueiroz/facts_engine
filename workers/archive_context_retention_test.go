package workers

import (
	"encoding/json"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func contextRetentionFixture(t *testing.T) (model.ArchivePayload, model.ArchivePayload) {
	t.Helper()
	text := "private fact"
	m := model.ContextManifest{ID: "context", Tenant: "tenant", Scope: "scope", Caller: model.Caller{Tenant: "tenant", Principal: "reader"}, CanonicalVersion: "context-manifest-v2", CreationRevision: 1, Request: model.ContextRequest{IncludeMemories: true, Query: model.RecallRequest{Scopes: []string{"scope"}, Query: "private query"}}, Items: []model.RecallItem{{Key: model.FactKey{Scope: "scope"}, Fact: model.FactResult{Value: &model.Value{Kind: model.Text, Text: &text}}}}, Memories: []model.MemoryRecallItem{{Key: model.MemoryKey{Scope: "scope"}, Memory: model.MemoryResult{Body: "private memory", Dependencies: []model.MemoryDependency{{Scope: "dependency", VersionID: "dep-v1", ContentDigest: "dependency-digest", Label: "private label"}}}}}, Rendered: []byte("private rendering")}
	m.RenderedDigest = model.BytesDigest(m.Rendered)
	m.Digest = model.ContextDigest(m)
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	before := model.ArchivePayload{Event: model.Event{ID: "event", Type: "prepare_context", Scope: "scope", Caller: m.Caller, LedgerRevision: 1}, Scopes: []string{"dependency", "scope"}, Entries: []model.AuditEntry{{Kind: "context", ID: m.ID, Payload: raw}}, Gaps: []model.AuditGap{{Kind: "evidence_content", Reason: "locators_only"}}}
	after := cloneArchivePayload(t, before)
	after.Entries[0].Payload, err = json.Marshal(model.ContextTombstone(m))
	if err != nil {
		t.Fatal(err)
	}
	after.Gaps = append(after.Gaps, model.AuditGap{Kind: "context_content", ID: m.ID, Reason: "disposed_under_retention_policy"})
	return before, after
}
func changeArchivedContext(t *testing.T, p *model.ArchivePayload, change func(*model.ContextManifest)) {
	t.Helper()
	var m model.ContextManifest
	if err := json.Unmarshal(p.Entries[0].Payload, &m); err != nil {
		t.Fatal(err)
	}
	change(&m)
	var err error
	p.Entries[0].Payload, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
}
func TestArchiveContextRetentionBoundaries(t *testing.T) {
	before, after := contextRetentionFixture(t)
	raw, _ := json.Marshal(before)
	if !archiveRetentionReplay(raw, after) {
		t.Fatal("valid context transition rejected")
	}
	for name, change := range map[string]func(*model.ContextManifest){
		"rendering":          func(m *model.ContextManifest) { m.Rendered = []byte("retained") },
		"query":              func(m *model.ContextManifest) { m.Request.Query.Query = "retained" },
		"fact":               func(m *model.ContextManifest) { m.Items[0].Fact.Value = &model.Value{Kind: model.Boolean} },
		"memory":             func(m *model.ContextManifest) { m.Memories[0].Memory.Body = "retained" },
		"label":              func(m *model.ContextManifest) { m.Memories[0].Memory.Dependencies[0].Label = "retained" },
		"dependency scope":   func(m *model.ContextManifest) { m.Memories[0].Memory.Dependencies[0].Scope = "other" },
		"dependency version": func(m *model.ContextManifest) { m.Memories[0].Memory.Dependencies[0].VersionID = "other" },
		"digest":             func(m *model.ContextManifest) { m.Digest = "other" },
		"rendered digest":    func(m *model.ContextManifest) { m.RenderedDigest = "other" },
		"caller":             func(m *model.ContextManifest) { m.Caller.Principal = "other" },
		"revision":           func(m *model.ContextManifest) { m.CreationRevision++ },
		"flag":               func(m *model.ContextManifest) { m.PayloadDisposed = false },
	} {
		t.Run(name, func(t *testing.T) {
			altered := cloneArchivePayload(t, after)
			changeArchivedContext(t, &altered, change)
			if archiveRetentionReplay(raw, altered) {
				t.Fatal("unrelated change accepted")
			}
		})
	}
	for name, change := range map[string]func(*model.ArchivePayload){
		"event":            func(p *model.ArchivePayload) { p.Event.Reason = "changed" },
		"scope":            func(p *model.ArchivePayload) { p.Scopes = []string{"scope"} },
		"missing gap":      func(p *model.ArchivePayload) { p.Gaps = p.Gaps[:1] },
		"duplicate gap":    func(p *model.ArchivePayload) { p.Gaps = append(p.Gaps, p.Gaps[1]) },
		"wrong gap kind":   func(p *model.ArchivePayload) { p.Gaps[1].Kind = "snapshot_content" },
		"wrong gap ID":     func(p *model.ArchivePayload) { p.Gaps[1].ID = "other" },
		"changed coverage": func(p *model.ArchivePayload) { p.Gaps[0].Reason = "changed" },
	} {
		t.Run(name, func(t *testing.T) {
			altered := cloneArchivePayload(t, after)
			change(&altered)
			if archiveRetentionReplay(raw, altered) {
				t.Fatal("unrelated archive change accepted")
			}
		})
	}
	for _, corrupt := range []func(*model.ContextManifest){
		func(m *model.ContextManifest) { m.Request.Query.Query = "corrupt" },
		func(m *model.ContextManifest) { m.RenderedDigest = "corrupt"; m.Digest = model.ContextDigest(*m) },
		func(m *model.ContextManifest) { m.CanonicalVersion = "unknown"; m.Digest = model.ContextDigest(*m) },
		func(m *model.ContextManifest) { m.PayloadDisposed = true },
	} {
		bad := cloneArchivePayload(t, before)
		changeArchivedContext(t, &bad, corrupt)
		badRaw, _ := json.Marshal(bad)
		if archiveRetentionReplay(badRaw, after) {
			t.Fatal("invalid original accepted")
		}
	}
	for _, original := range []bool{false, true} {
		badBefore, badAfter := cloneArchivePayload(t, before), cloneArchivePayload(t, after)
		entry := &badAfter.Entries[0]
		if original {
			entry = &badBefore.Entries[0]
		}
		entry.Payload = append([]byte(`{"UnknownContent":"private",`), entry.Payload[1:]...)
		badRaw, _ := json.Marshal(badBefore)
		if archiveRetentionReplay(badRaw, badAfter) {
			t.Fatal("unknown retained content accepted")
		}
	}
	if archiveRetentionReplay(raw, before) {
		t.Fatal("no transition accepted")
	}
}
