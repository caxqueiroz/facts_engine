package services

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

type contextPermissionAuthorizer struct{ denyScope, denyPermission string }

func (a contextPermissionAuthorizer) Check(_ context.Context, r model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	return model.AuthorizationDecision{Allowed: r.Scope != a.denyScope || r.Permission != a.denyPermission, ModelID: "model", Consistency: "HIGHER_CONSISTENCY", CheckedAt: time.Now()}, nil
}
func TestMemoryContextDependencyDisclosure(t *testing.T) {
	ctx := context.Background()
	c := model.Caller{Tenant: "org", Principal: "reader"}
	m := model.ContextManifest{ID: "manifest", Request: model.ContextRequest{Query: model.RecallRequest{Scopes: []string{"case"}}}, Memories: []model.MemoryRecallItem{{Key: model.MemoryKey{Scope: "case", ID: "summary"}, Memory: model.MemoryResult{Body: "private body", Dependencies: []model.MemoryDependency{{Scope: "dependency"}}}}}}
	if !slices.Equal(model.ContextScopes(m), []string{"case", "dependency"}) {
		t.Fatal("dependency missing from retained scopes")
	}
	for _, permission := range []string{"can_read", "can_read_history", "can_read_evidence", "can_prepare_context"} {
		s := &Service{auth: contextPermissionAuthorizer{"dependency", permission}}
		if _, err := s.contextMemoryAuthorization(ctx, c, m, "review", true); !errors.Is(err, model.ErrDenied) {
			t.Fatal("dependency permission omitted", permission, err)
		}
		if permission != "can_prepare_context" {
			if _, err := s.contextMemoryAuthorization(ctx, c, m, "review", false); !errors.Is(err, model.ErrDenied) {
				t.Fatal("retained dependency permission omitted", permission, err)
			}
		}
	}
	s := &Service{}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	entries := []model.AuditEntry{{Kind: "context", ID: m.ID, Payload: raw}}
	for _, full := range []bool{false, true} {
		out, gaps, err := s.prepareAuditEntries(ctx, c, model.AuditExportRequest{Scope: "case", IncludeEvidence: full}, entries)
		if err != nil || len(out) != 0 || len(gaps) != 1 {
			t.Fatal("cross-scope memory context disclosed", out, gaps, err)
		}
		if full && gaps[0].Reason != "outside_export_scope" {
			t.Fatal(gaps)
		}
		if !full && gaps[0].Reason != "memory_context_payload_withheld" {
			t.Fatal(gaps)
		}
	}
}
