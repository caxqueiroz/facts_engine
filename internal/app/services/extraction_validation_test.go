package services

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestExtractionParserRejectsAmbiguousEnvelopes(t *testing.T) {
	for _, body := range []string{`{}`, `{"candidates":null}`, `{"candidates":[],"tool":"accept"}`, `{"candidates":[]} {"candidates":[]}`, `{"candidates":[{},{}]}`} {
		if _, err := parseExtraction([]byte(body), 1); err == nil {
			t.Errorf("accepted %s", body)
		}
	}
	if _, err := parseExtraction([]byte(`{"candidates":[]}`), 1); err != nil {
		t.Fatal(err)
	}
}

func TestExtractionAuditReportsMissingAndPartialResponses(t *testing.T) {
	payload, err := json.Marshal(model.ExtractionResult{AttemptID: "partial", ResponseComplete: false})
	if err != nil {
		t.Fatal(err)
	}
	entries := []model.AuditEntry{
		{Kind: "extraction_attempt", ID: "missing", Payload: []byte(`{}`)},
		{Kind: "extraction_attempt", ID: "partial", Payload: []byte(`{}`)},
		{Kind: "extraction_result", ID: "partial", Payload: payload},
	}
	s := &Service{}
	out, gaps, err := s.prepareAuditEntries(context.Background(), model.Caller{}, model.AuditExportRequest{IncludeEvidence: true}, entries)
	if err != nil || len(out) != 3 || len(gaps) != 2 {
		t.Fatalf("out=%v gaps=%v err=%v", out, gaps, err)
	}
	reasons := map[string]string{}
	for _, gap := range gaps {
		reasons[gap.ID] = gap.Reason
	}
	if reasons["missing"] != "response_not_retained_at_export_revision" || reasons["partial"] != "complete_response_not_retained" {
		t.Fatal(gaps)
	}
	out, gaps, err = s.prepareAuditEntries(context.Background(), model.Caller{}, model.AuditExportRequest{}, entries)
	if err != nil || len(out) != 0 || len(gaps) != 3 {
		t.Fatalf("metadata out=%v gaps=%v err=%v", out, gaps, err)
	}
}
