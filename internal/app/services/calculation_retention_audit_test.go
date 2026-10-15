package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestCalculationTombstoneAuditCoverage(t *testing.T) {
	for _, mode := range []string{"included", "cross-scope", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			calc := model.CalculationTombstone(model.CalculationRecord{ID: "calc", Tenant: "tenant", Receipt: model.CalculationReceipt{Scope: "scope", SnapshotID: "snapshot", Input: model.ArtifactReference{EvidenceID: "input"}, Output: model.ArtifactReference{EvidenceID: "output"}, MethodID: "private-method"}})
			snapshot := model.FactSnapshot{ID: "snapshot", Items: []model.SnapshotItem{{Selector: model.FactKey{Scope: "scope"}}}}
			if mode == "cross-scope" {
				snapshot.Items[0].Selector.Scope = "private"
			}
			if mode == "malformed" {
				calc.Receipt.MethodID = "private-method"
			}
			entries := []model.AuditEntry{}
			for _, item := range []struct {
				kind, id string
				value    any
			}{{"snapshot", "snapshot", snapshot}, {"evidence", "input", model.Evidence{ID: "input"}}, {"evidence", "output", model.Evidence{ID: "output"}}, {"calculation", "calc", calc}} {
				payload, err := json.Marshal(item.value)
				if err != nil {
					t.Fatal(err)
				}
				entries = append(entries, model.AuditEntry{Kind: item.kind, ID: item.id, Payload: payload})
			}
			s := &Service{}
			out, gaps, err := s.prepareAuditEntries(context.Background(), model.Caller{Tenant: "tenant"}, model.AuditExportRequest{Scope: "scope"}, entries)
			if mode == "malformed" {
				if !errors.Is(err, model.ErrPrecondition) {
					t.Fatal("malformed metadata exported", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			included, coverage := false, false
			for _, entry := range out {
				if entry.Kind == "calculation" {
					included = true
				}
			}
			for _, gap := range gaps {
				if gap.Kind == "calculation_content" && gap.ID == "calc" && gap.Reason == "disposed_under_retention_policy" {
					coverage = true
				}
			}
			if included != (mode == "included") || coverage != (mode == "included") {
				t.Fatal(out, gaps)
			}
		})
	}
}
