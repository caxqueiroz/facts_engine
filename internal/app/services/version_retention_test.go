package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestAuditVersionDisposalBoundaries(t *testing.T) {
	caller := model.Caller{Tenant: "tenant", Principal: "reader"}
	for _, memory := range []bool{false, true} {
		for _, full := range []bool{false, true} {
			kind := "version"
			var metadata any = model.Version{ID: "v", PayloadDisposed: true, Proposer: caller, Proposal: model.FactProposal{Key: model.FactKey{Scope: "scope"}}}
			if memory {
				kind = "memory_version"
				metadata = model.MemoryVersion{ID: "v", PayloadDisposed: true, Proposer: caller, Proposal: model.MemoryProposal{Key: model.MemoryKey{Scope: "scope", ID: "summary"}, Dependencies: []model.MemoryDependency{{Scope: "scope", RecordID: "fact", VersionID: "version", Label: "uncertain"}}}}
			}
			raw, _ := json.Marshal(metadata)
			request := model.AuditExportRequest{Scope: "scope", IncludeEvidence: full}
			entry := model.AuditEntry{Kind: kind, ID: "v", Payload: raw}
			out, gaps, err := (&Service{}).prepareAuditEntries(context.Background(), caller, request, []model.AuditEntry{entry})
			if err != nil || len(out) != 1 || len(gaps) != 1 || gaps[0].Kind != kind+"_content" || gaps[0].Reason != "disposed_under_retention_policy" {
				t.Fatal("missing metadata coverage", memory, full, out, gaps, err)
			}
			for _, change := range []func(map[string]any){
				func(v map[string]any) { v["ID"] = "other" },
				func(v map[string]any) { v["Proposer"].(map[string]any)["Tenant"] = "other" },
				func(v map[string]any) {
					v["Proposal"].(map[string]any)["Sources"] = []any{map[string]any{"Quote": "private"}}
				},
				func(v map[string]any) {
					p := v["Proposal"].(map[string]any)
					if memory {
						p["Body"] = "private"
					} else {
						p["Value"].(map[string]any)["Text"] = "private"
					}
				},
			} {
				var v map[string]any
				if err := json.Unmarshal(raw, &v); err != nil {
					t.Fatal(err)
				}
				change(v)
				entry.Payload, _ = json.Marshal(v)
				if _, _, err := (&Service{}).prepareAuditEntries(context.Background(), caller, request, []model.AuditEntry{entry}); !errors.Is(err, model.ErrPrecondition) {
					t.Fatal("malformed tombstone accepted", memory, full, err)
				}
			}
			if memory {
				v := metadata.(model.MemoryVersion)
				v.Proposal.Dependencies[0].Scope = "other"
				entry.Payload, _ = json.Marshal(v)
				out, gaps, err := (&Service{}).prepareAuditEntries(context.Background(), caller, request, []model.AuditEntry{entry})
				if err != nil || len(out) != 0 || len(gaps) != 1 || gaps[0].Reason != "outside_export_scope" {
					t.Fatal("cross-scope tombstone disclosed", out, gaps, err)
				}
			}
		}
	}
}
