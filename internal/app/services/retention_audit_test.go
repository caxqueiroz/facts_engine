package services

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestDisposalAuditCoversLineageAndDeduplicatesPriorDisposal(t *testing.T) {
	events := []model.RetentionEvent{
		{Kind: "dispose_evidence", TargetID: "child"}, // legacy singleton event
		{Kind: "dispose_evidence", TargetID: "parent", Disposal: &model.EvidenceDisposalRequest{EvidenceID: "parent", DerivativeEvidenceIDs: []string{"child", "other-child"}}},
	}
	entries := []model.AuditEntry{}
	for _, event := range events {
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, model.AuditEntry{Kind: "retention", ID: event.TargetID, Payload: raw})
	}
	s := &Service{}
	out, gaps, err := s.prepareAuditEntries(context.Background(), model.Caller{}, model.AuditExportRequest{IncludeEvidence: true}, entries)
	if err != nil || len(out) != 2 || len(gaps) != 3 {
		t.Fatal(out, gaps, err)
	}
	seen := map[string]bool{}
	for _, gap := range gaps {
		if seen[gap.ID] || gap.Kind != "evidence_content" || gap.Reason != "disposed_under_retention_policy" {
			t.Fatal(gap)
		}
		seen[gap.ID] = true
	}
	for _, id := range []string{"parent", "child", "other-child"} {
		if !seen[id] {
			t.Fatal("missing coverage", id)
		}
	}
}
