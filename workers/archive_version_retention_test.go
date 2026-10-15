package workers

import (
	"encoding/json"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func versionRetentionFixture(t *testing.T, memory bool) (model.ArchivePayload, model.ArchivePayload) {
	t.Helper()
	text := "private value"
	caller := model.Caller{Tenant: "tenant", Principal: "proposer"}
	fact := model.Version{ID: "version", RecordID: "record", PolicyID: "policy", Proposer: caller, Proposal: model.FactProposal{Key: model.FactKey{Scope: "scope", Subject: "subject", Predicate: "predicate"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &text}, Sources: []model.SourceSpan{{EvidenceID: "source", Start: 2, End: 10, Quote: "private quote"}}}}
	fact.Digest = model.FactDigest(fact.Proposal)
	var original, disposed any = fact, model.VersionTombstone(fact)
	kind, recordKind := "version", ""
	if memory {
		v := model.MemoryVersion{ID: fact.ID, RecordID: fact.RecordID, PolicyID: fact.PolicyID, Proposer: caller, LedgerRevision: 1, Proposal: model.MemoryProposal{Key: model.MemoryKey{Scope: "scope", ID: "summary"}, Subtype: "episode", SchemaVersion: "v1", Body: "private body", Sources: fact.Proposal.Sources, Dependencies: []model.MemoryDependency{{Scope: "dependency", RecordID: "fact", VersionID: "fact-v1", ContentDigest: "original", Label: "uncertain"}}}}
		v.Digest = model.MemoryDigest(v.Proposal)
		original, disposed = v, model.MemoryVersionTombstone(v)
		kind, recordKind = "memory_version", "memory"
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	before := model.ArchivePayload{Event: model.Event{ID: "event", Type: "propose_fact", Scope: "scope", RecordID: "record", VersionID: "version", RecordKind: recordKind, Caller: caller, LedgerRevision: 1}, Scopes: []string{"dependency", "scope"}, Entries: []model.AuditEntry{{Kind: kind, ID: "version", Payload: raw}}, Gaps: []model.AuditGap{{Kind: "evidence_content", Reason: "locators_only"}}}
	if memory {
		before.Event.Type = "propose_memory"
	}
	after := cloneArchivePayload(t, before)
	after.Entries[0].Payload, err = json.Marshal(disposed)
	if err != nil {
		t.Fatal(err)
	}
	after.Gaps = append(after.Gaps, model.AuditGap{Kind: kind + "_content", ID: "version", Reason: "disposed_under_retention_policy"})
	return before, after
}

func changeArchivedVersion(t *testing.T, p *model.ArchivePayload, change func(map[string]any)) {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(p.Entries[0].Payload, &v); err != nil {
		t.Fatal(err)
	}
	change(v)
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	p.Entries[0].Payload = raw
}

func TestArchiveVersionRetentionBoundaries(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name := "fact"
		if memory {
			name = "memory"
		}
		t.Run(name, func(t *testing.T) {
			before, after := versionRetentionFixture(t, memory)
			raw, _ := json.Marshal(before)
			if !archiveRetentionReplay(raw, after) {
				t.Fatal("exact transition rejected")
			}
			changes := map[string]func(map[string]any){
				"flag":     func(v map[string]any) { v["PayloadDisposed"] = false },
				"digest":   func(v map[string]any) { v["Digest"] = "other" },
				"record":   func(v map[string]any) { v["RecordID"] = "other" },
				"version":  func(v map[string]any) { v["ID"] = "other" },
				"policy":   func(v map[string]any) { v["PolicyID"] = "other" },
				"proposer": func(v map[string]any) { v["Proposer"].(map[string]any)["Principal"] = "other" },
				"scope":    func(v map[string]any) { v["Proposal"].(map[string]any)["Key"].(map[string]any)["Scope"] = "other" },
				"schema":   func(v map[string]any) { v["Proposal"].(map[string]any)["SchemaVersion"] = "other" },
				"quote": func(v map[string]any) {
					v["Proposal"].(map[string]any)["Sources"].([]any)[0].(map[string]any)["Quote"] = "private"
				},
				"locator": func(v map[string]any) {
					v["Proposal"].(map[string]any)["Sources"].([]any)[0].(map[string]any)["Start"] = 3
				},
				"unknown": func(v map[string]any) { v["UnknownContent"] = "private" },
			}
			if memory {
				changes["body"] = func(v map[string]any) { v["Proposal"].(map[string]any)["Body"] = "private" }
				changes["dependency"] = func(v map[string]any) {
					v["Proposal"].(map[string]any)["Dependencies"].([]any)[0].(map[string]any)["ContentDigest"] = "other"
				}
				changes["label"] = func(v map[string]any) {
					v["Proposal"].(map[string]any)["Dependencies"].([]any)[0].(map[string]any)["Label"] = "assertion"
				}
				changes["revision"] = func(v map[string]any) { v["LedgerRevision"] = 2 }
			} else {
				changes["value"] = func(v map[string]any) { v["Proposal"].(map[string]any)["Value"].(map[string]any)["Text"] = "private" }
				changes["extraction"] = func(v map[string]any) { v["ExtractionAttemptID"] = "other" }
			}
			for label, change := range changes {
				t.Run(label, func(t *testing.T) {
					altered := cloneArchivePayload(t, after)
					changeArchivedVersion(t, &altered, change)
					if archiveRetentionReplay(raw, altered) {
						t.Fatal("changed metadata/content accepted")
					}
				})
			}
			for label, change := range map[string]func(*model.ArchivePayload){
				"event":         func(p *model.ArchivePayload) { p.Event.Reason = "other" },
				"scopes":        func(p *model.ArchivePayload) { p.Scopes = []string{"scope"} },
				"missing gap":   func(p *model.ArchivePayload) { p.Gaps = p.Gaps[:1] },
				"duplicate gap": func(p *model.ArchivePayload) { p.Gaps = append(p.Gaps, p.Gaps[1]) },
				"wrong gap":     func(p *model.ArchivePayload) { p.Gaps[1].Kind = "context_content" },
			} {
				t.Run(label, func(t *testing.T) {
					altered := cloneArchivePayload(t, after)
					change(&altered)
					if archiveRetentionReplay(raw, altered) {
						t.Fatal("unrelated archive change accepted")
					}
				})
			}
			for _, change := range []func(map[string]any){
				func(v map[string]any) { v["PayloadDisposed"] = true },
				func(v map[string]any) { v["Digest"] = "corrupt" },
				func(v map[string]any) { v["UnknownContent"] = "private" },
			} {
				altered := cloneArchivePayload(t, before)
				changeArchivedVersion(t, &altered, change)
				badRaw, _ := json.Marshal(altered)
				if archiveRetentionReplay(badRaw, after) {
					t.Fatal("invalid original accepted")
				}
			}
			// Matching mutations in both envelopes must still respect the event binding.
			for _, change := range []func(map[string]any){
				func(v map[string]any) { v["ID"] = "wrong" },
				func(v map[string]any) { v["RecordID"] = "wrong" },
				func(v map[string]any) { v["Proposer"].(map[string]any)["Tenant"] = "wrong" },
			} {
				old, current := cloneArchivePayload(t, before), cloneArchivePayload(t, after)
				changeArchivedVersion(t, &old, change)
				changeArchivedVersion(t, &current, change)
				badRaw, _ := json.Marshal(old)
				if archiveRetentionReplay(badRaw, current) {
					t.Fatal("unbound version accepted")
				}
			}
			if archiveRetentionReplay(raw, before) {
				t.Fatal("no transition accepted")
			}
			testArchiveRetentionRetry(t, before, after)
		})
	}
}
