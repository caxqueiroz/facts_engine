package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestExtractionTombstoneAuditBoundaries(t *testing.T) {
	caller := model.Caller{Tenant: "tenant", Principal: "reader"}
	for _, kind := range []string{"extraction_job", "extraction_attempt", "extraction_result"} {
		var value any
		switch kind {
		case "extraction_job":
			value = model.ExtractionJobTombstone(model.ExtractionJob{ID: "artifact", Tenant: caller.Tenant, Request: model.ExtractionRequest{Scope: "scope"}})
		case "extraction_attempt":
			value = model.ExtractionAttemptTombstone(model.ExtractionAttempt{ID: "artifact", Tenant: caller.Tenant, Scope: "scope"})
		case "extraction_result":
			value = model.ExtractionResultTombstone(model.ExtractionResult{AttemptID: "artifact", Tenant: caller.Tenant, Scope: "scope", ResponseComplete: true})
		}
		raw, _ := json.Marshal(value)
		if !model.ValidExtractionTombstoneJSON(kind, raw) {
			t.Fatal("valid metadata rejected", kind)
		}
		for _, full := range []bool{false, true} {
			request := model.AuditExportRequest{Scope: "scope", IncludeEvidence: full}
			// An attempt without its result has a separate missing-response gap.
			entry := model.AuditEntry{Kind: kind, ID: "artifact", Payload: raw}
			out, gaps, err := (&Service{}).prepareAuditEntries(context.Background(), caller, request, []model.AuditEntry{entry})
			found := false
			for _, gap := range gaps {
				if gap.Kind == kind+"_content" && gap.Reason == "disposed_under_retention_policy" {
					found = true
				}
			}
			if err != nil || len(out) != 1 || !found {
				t.Fatal("disposed metadata missing", kind, full, out, gaps, err)
			}
			for _, change := range []func(map[string]any){
				func(v map[string]any) { v["Tenant"] = "other" },
				func(v map[string]any) { v["UnknownContent"] = "private" },
				func(v map[string]any) {
					switch kind {
					case "extraction_job":
						v["Profile"].(map[string]any)["Prompt"] = []byte("private")
					case "extraction_attempt":
						v["RequestBytes"] = []byte("private")
					case "extraction_result":
						v["ResponseBytes"] = []byte("private")
					}
				},
			} {
				var v map[string]any
				_ = json.Unmarshal(raw, &v)
				change(v)
				entry.Payload, _ = json.Marshal(v)
				if _, _, err := (&Service{}).prepareAuditEntries(context.Background(), caller, request, []model.AuditEntry{entry}); !errors.Is(err, model.ErrPrecondition) {
					t.Fatal("malformed metadata disclosed", kind, err)
				}
			}
			var v map[string]any
			_ = json.Unmarshal(raw, &v)
			v["ContentScopes"] = []string{"other"}
			entry.Payload, _ = json.Marshal(v)
			out, gaps, err = (&Service{}).prepareAuditEntries(context.Background(), caller, request, []model.AuditEntry{entry})
			if err != nil || len(out) != 0 {
				t.Fatal("cross-scope artifact disclosed", kind, err)
			}
			for _, gap := range gaps {
				if gap.Kind == kind+"_content" {
					t.Fatal("cross-scope disposal state disclosed")
				}
			}
		}
	}
}
