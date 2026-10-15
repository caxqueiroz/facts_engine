package workers

import (
	"encoding/json"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func extractionRetentionFixture(t *testing.T, kind string, complete bool) (model.ArchivePayload, model.ArchivePayload) {
	t.Helper()
	caller := model.Caller{Tenant: "tenant", Principal: "worker"}
	event := model.Event{ID: "event", EntityID: "job", Scope: "scope", Caller: caller, LedgerRevision: 1}
	id := "job"
	var from, to any
	switch kind {
	case "extraction_job":
		text := "private value"
		v := model.Version{ID: "v", RecordID: "record", Proposal: model.FactProposal{Key: model.FactKey{Scope: "dependency"}, Value: model.Value{Kind: model.Text, Text: &text}, Sources: []model.SourceSpan{{EvidenceID: "source", Quote: "private quote"}}}}
		v.Digest = model.FactDigest(v.Proposal)
		j := model.ExtractionJob{ID: id, Tenant: "tenant", Caller: caller, Request: model.ExtractionRequest{Scope: "scope"}, CreatedRevision: 1, Profile: model.ExtractionProfile{ID: "profile", Prompt: []byte("private prompt"), Parameters: json.RawMessage(`{"private":"parameter"}`)}, DependencyVersions: []model.Version{v}}
		j.ProfileDigest = model.ExtractionProfileDigest(j.Profile)
		from, to = j, model.ExtractionJobTombstone(j)
		event.Type = "start_extraction"
	case "extraction_attempt":
		id = "attempt"
		event.EntityID = id
		event.Type = "begin_extraction"
		a := model.ExtractionAttempt{ID: id, JobID: "job", Tenant: "tenant", Scope: "scope", LedgerRevision: 1, RequestBytes: []byte("private request"), OwnerNonce: "nonce"}
		a.RequestDigest = model.BytesDigest(a.RequestBytes)
		from, to = a, model.ExtractionAttemptTombstone(a)
	case "extraction_result":
		id = "attempt"
		event.Type = "finish_extraction"
		r := model.ExtractionResult{AttemptID: id, JobID: "job", Tenant: "tenant", Scope: "scope", LedgerRevision: 1, ResponseBytes: []byte("private response"), ResponseComplete: complete, RequestDigest: model.BytesDigest([]byte("request")), Outcome: "completed", Proposals: []model.ExtractedProposal{{RecordID: "record", VersionID: "version"}}}
		r.ResponseDigest = model.BytesDigest(r.ResponseBytes)
		if !complete {
			r.ResponseBytes = r.ResponseBytes[:4]
			r.Outcome = "response_limit"
		}
		from, to = r, model.ExtractionResultTombstone(r)
	}
	raw, err := json.Marshal(from)
	if err != nil {
		t.Fatal(err)
	}
	before := model.ArchivePayload{Event: event, Scopes: []string{"dependency", "scope"}, Entries: []model.AuditEntry{{Kind: kind, ID: id, Payload: raw}}, Gaps: []model.AuditGap{{Kind: "evidence_content", Reason: "locators_only"}}}
	after := cloneArchivePayload(t, before)
	after.Entries[0].Payload, err = json.Marshal(to)
	if err != nil {
		t.Fatal(err)
	}
	after.Gaps = append(after.Gaps, model.AuditGap{Kind: kind + "_content", ID: id, Reason: "disposed_under_retention_policy"})
	return before, after
}

func TestArchiveExtractionRetention(t *testing.T) {
	for _, kind := range []string{"extraction_job", "extraction_attempt", "extraction_result"} {
		t.Run(kind, func(t *testing.T) {
			before, after := extractionRetentionFixture(t, kind, true)
			raw, _ := json.Marshal(before)
			if !archiveRetentionReplay(raw, after) {
				t.Fatal("exact transition rejected")
			}
			for _, change := range []func(map[string]any){
				func(v map[string]any) { v["PayloadDisposed"] = false },
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
				func(v map[string]any) {
					switch kind {
					case "extraction_job":
						v["DependencyVersions"].([]any)[0].(map[string]any)["RecordID"] = "other"
					case "extraction_attempt":
						v["OwnerNonce"] = "other"
					case "extraction_result":
						v["ResponseComplete"] = false
					}
				},
			} {
				altered := cloneArchivePayload(t, after)
				changeArchivedVersion(t, &altered, change)
				if archiveRetentionReplay(raw, altered) {
					t.Fatal("unrelated artifact change accepted")
				}
			}
			for _, change := range []func(*model.ArchivePayload){
				func(p *model.ArchivePayload) { p.Gaps = p.Gaps[:1] },
				func(p *model.ArchivePayload) { p.Gaps = append(p.Gaps, p.Gaps[1]) },
				func(p *model.ArchivePayload) { p.Scopes = []string{"scope"} },
				func(p *model.ArchivePayload) { p.Event.Reason = "other" },
			} {
				altered := cloneArchivePayload(t, after)
				change(&altered)
				if archiveRetentionReplay(raw, altered) {
					t.Fatal("coverage/event change accepted")
				}
			}
			for _, field := range []string{"digest", "unknown", "flag"} {
				original := cloneArchivePayload(t, before)
				changeArchivedVersion(t, &original, func(v map[string]any) {
					switch field {
					case "unknown":
						v["UnknownContent"] = "private"
					case "flag":
						v["PayloadDisposed"] = true
					case "digest":
						key := "ProfileDigest"
						if kind == "extraction_attempt" {
							key = "RequestDigest"
						}
						if kind == "extraction_result" {
							key = "ResponseDigest"
						}
						v[key] = "corrupt"
					}
				})
				bad, _ := json.Marshal(original)
				if archiveRetentionReplay(bad, after) {
					t.Fatal("invalid original accepted", field)
				}
			}
			badBefore, badAfter := cloneArchivePayload(t, before), cloneArchivePayload(t, after)
			badBefore.Event.EntityID = "other"
			badAfter.Event.EntityID = "other"
			bad, _ := json.Marshal(badBefore)
			if archiveRetentionReplay(bad, badAfter) {
				t.Fatal("artifact not bound to event")
			}
			testArchiveRetentionRetry(t, before, after)
		})
	}
	t.Run("truncated response", func(t *testing.T) {
		before, after := extractionRetentionFixture(t, "extraction_result", false)
		raw, _ := json.Marshal(before)
		if !archiveRetentionReplay(raw, after) {
			t.Fatal("truncated response transition rejected")
		}
		testArchiveRetentionRetry(t, before, after)
	})
}
