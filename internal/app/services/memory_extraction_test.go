package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestMemoryExtractionCandidateBoundaries(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	input := model.ExtractionInput{MemoryTargets: []model.MemoryExtractionTarget{{ID: "episode", Subtype: "summary", SchemaVersion: "v1"}}, Messages: []model.ExtractionInputMessage{{EvidenceID: "source", Content: "tea", SpeakerRole: "customer", SourceClass: "customer"}, {EvidenceID: "assistant", Content: "tea", SpeakerRole: "assistant", SourceClass: "assistant"}}}
	valid := model.MemoryExtractionCandidate{ID: "episode", Subtype: "summary", SchemaVersion: "v1", Body: "Customer prefers tea.", Valid: model.Interval{From: &from}, Sources: []model.SourceSpan{{EvidenceID: "source", Start: 0, End: 3, Quote: "tea"}}, Uncertainty: "asserted", Confidence: .5}
	if code := memoryCandidateIssue(valid, input); code != "" {
		t.Fatal(code)
	}
	for _, field := range []string{"identity", "subtype", "schema", "body", "time", "uncertainty", "confidence", "source", "span", "assistant"} {
		candidate := valid
		candidate.Sources = append([]model.SourceSpan{}, valid.Sources...)
		switch field {
		case "identity":
			candidate.ID = "invented"
		case "subtype":
			candidate.Subtype = "unapproved"
		case "schema":
			candidate.SchemaVersion = "v2"
		case "body":
			candidate.Body = ""
		case "time":
			candidate.Valid.From = nil
		case "uncertainty":
			candidate.Uncertainty = "inferred"
		case "confidence":
			candidate.Confidence = 2
		case "source":
			candidate.Sources[0].EvidenceID = "unsupplied"
		case "span":
			candidate.Sources[0].Quote = "coffee"
		case "assistant":
			candidate.Sources[0].EvidenceID = "assistant"
		}
		if code := memoryCandidateIssue(candidate, input); code == "" {
			t.Fatal("invalid candidate accepted", field)
		}
	}
	raw, err := json.Marshal(model.MemoryExtractionOutput{Candidates: []model.MemoryExtractionCandidate{valid}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = parseMemoryExtraction(raw, 1); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{}`, `{"candidates":[{"ID":"episode","Dependencies":[]}]}`, `{"candidates":[]} {}`, `{"candidates":[],"approve":true}`} {
		if _, err = parseMemoryExtraction([]byte(raw), 1); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("unapproved output schema accepted", raw, err)
		}
	}
	if _, err = parseMemoryExtraction([]byte(`{"candidates":[{},{}]}`), 1); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("candidate bound ignored", err)
	}
}

func TestMemoryExtractionArtifactScopes(t *testing.T) {
	ctx := context.Background()
	caller := model.Caller{Tenant: "bank", Principal: "reader"}
	worker := model.Caller{Tenant: "bank", Principal: "worker", Service: true}
	job := model.ExtractionJob{Caller: caller, Request: model.ExtractionRequest{Command: model.Command{Purpose: "review"}, Scope: "own", MemoryTargets: []model.MemoryExtractionTarget{{ID: "summary"}}}, ContentScopes: []string{"own", "dependency"}}
	for _, permission := range []string{"can_read", "can_read_history", "can_read_evidence"} {
		s := &Service{auth: contextPermissionAuthorizer{denyScope: "dependency", denyPermission: permission}}
		if _, err := s.extractionAuthorizations(ctx, worker, job); !errors.Is(err, model.ErrDenied) {
			t.Fatal("memory extraction dependency permission bypass", permission, err)
		}
	}
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"extraction_job", "extraction_attempt", "extraction_result"} {
		entries, gaps, err := (&Service{}).prepareAuditEntries(ctx, caller, model.AuditExportRequest{Scope: "own", IncludeEvidence: true}, []model.AuditEntry{{Kind: kind, ID: "artifact", Payload: raw}})
		redacted := false
		for _, gap := range gaps {
			if gap.Kind == kind && gap.ID == "artifact" && gap.Reason == "outside_export_scope" {
				redacted = true
			}
		}
		if err != nil || len(entries) != 0 || !redacted {
			t.Fatal("cross-scope memory artifacts disclosed", kind, entries, gaps, err)
		}
	}
}
