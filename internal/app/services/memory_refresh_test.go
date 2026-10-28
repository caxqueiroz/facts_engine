package services

import (
	"context"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestMemoryRefreshOutputValidation(t *testing.T) {
	for _, raw := range []string{`{}`, `{"Body":"summary","Dependencies":[]}`, `{"Body":"summary","DeclineReason":"cannot refresh"}`, `{"DeclineReason":"cannot refresh","accept":true}`, `{"DeclineReason":"cannot refresh"} {}`} {
		if _, err := parseMemoryRefresh([]byte(raw)); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("unsafe refresh output accepted", raw, err)
		}
	}
	if _, err := parseMemoryRefresh([]byte(`{"DeclineReason":"conflicting evidence"}`)); err != nil {
		t.Fatal(err)
	}
	valid := model.MemoryDependency{Scope: "scope", RecordID: "record", VersionID: "version", ContentDigest: "digest", Label: "assertion"}
	input := model.MemoryRefreshInput{Dependencies: []model.MemoryRefreshDependency{{Scope: "scope", RecordID: "record", Versions: []model.Version{{ID: "version", Digest: "digest"}}}}}
	if code := refreshDependencyIssue(input, model.MemoryRefreshOutput{Dependencies: []model.MemoryDependency{valid}}); code != "" {
		t.Fatal(code)
	}
	for _, field := range []string{"omitted", "duplicate", "scope", "record", "version", "digest", "label"} {
		changed := valid
		switch field {
		case "scope":
			changed.Scope = "other"
		case "record":
			changed.RecordID = "other"
		case "version":
			changed.VersionID = "other"
		case "digest":
			changed.ContentDigest = "other"
		case "label":
			changed.Label = "confident"
		}
		deps := []model.MemoryDependency{changed}
		if field == "omitted" {
			deps = nil
		}
		if field == "duplicate" {
			deps = append(deps, changed)
		}
		if code := refreshDependencyIssue(input, model.MemoryRefreshOutput{Dependencies: deps}); code == "" {
			t.Fatal("unbound refresh provenance accepted", field)
		}
	}
}

func TestMemoryRefreshDependencyAuthorization(t *testing.T) {
	job := model.ExtractionJob{Caller: model.Caller{Tenant: "org", Principal: "requester"}, Request: model.ExtractionRequest{Command: model.Command{Purpose: "review"}, Scope: "own", Refresh: &model.VersionReference{RecordID: "record", VersionID: "version"}}, RefreshScopes: []string{"own", "dependency"}}
	worker := model.Caller{Tenant: "org", Principal: "worker", Service: true}
	for _, permission := range []string{"can_read", "can_read_history", "can_read_evidence"} {
		s := &Service{auth: contextPermissionAuthorizer{denyScope: "dependency", denyPermission: permission}}
		if _, err := s.extractionAuthorizations(context.Background(), worker, job); !errors.Is(err, model.ErrDenied) {
			t.Fatal("refresh dependency permission bypass", permission, err)
		}
	}
}
