package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestDecisionTombstoneAuditCoverage(t *testing.T) {
	for _, mode := range []string{"included", "cross-scope", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			decision := model.DecisionTombstone(model.DecisionRecord{ID: "decision", Tenant: "tenant", Scopes: []string{"scope"}, Receipt: model.DecisionReceipt{Scope: "scope", MethodID: "private-method"}})
			if mode == "cross-scope" {
				decision.Scopes = append(decision.Scopes, "private")
			}
			if mode == "malformed" {
				decision.Receipt.MethodID = "private-method"
			}
			payload, err := json.Marshal(decision)
			if err != nil {
				t.Fatal(err)
			}
			s := &Service{}
			out, gaps, err := s.prepareAuditEntries(context.Background(), model.Caller{Tenant: "tenant"}, model.AuditExportRequest{Scope: "scope"}, []model.AuditEntry{{Kind: "decision", ID: decision.ID, Payload: payload}})
			if mode == "malformed" {
				if !errors.Is(err, model.ErrPrecondition) {
					t.Fatal("malformed metadata exported", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			coverage := false
			for _, gap := range gaps {
				if gap.Kind == "decision_content" && gap.ID == decision.ID && gap.Reason == "disposed_under_retention_policy" {
					coverage = true
				}
			}
			if (len(out) == 1) != (mode == "included") || coverage != (mode == "included") {
				t.Fatal(out, gaps)
			}
		})
	}
}
