package postgres_test

import (
	"connectrpc.com/connect/v2"
	"context"
	facts "github.com/caxqueiroz/facts_engine"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/protobuf/proto"
	"testing"
)

func testRemoteIdentities(t *testing.T, ctx context.Context, e, denied *facts.Engine, c model.Caller) {
	t.Helper()
	client := memoryv1connect.NewIdentityServiceClient(governanceClient(t, e, c))
	blocked := memoryv1connect.NewIdentityServiceClient(governanceClient(t, denied, c))
	cmd := func(key string) *v1.Command { return &v1.Command{Key: key, Purpose: "identity-administration"} }
	scope := &v1.PutScopeRequest{Tenant: c.Tenant, Command: cmd("remote-scope"), Definition: &v1.ScopeDefinition{Id: "remote-case", Label: "Remote case"}, Reason: "Create scope"}
	if _, err := blocked.PutScope(ctx, scope); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("scope admin: %v", err)
	}
	created, err := client.PutScope(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := e.PutScope(ctx, c, model.ScopeRequest{Command: model.Command{Key: scope.Command.Key, Purpose: scope.Command.Purpose}, Definition: model.ScopeDefinition{ID: scope.Definition.Id, Label: scope.Definition.Label}, Reason: scope.Reason})
	if err != nil || direct.ID != created.Result.Id || direct.LedgerRevision != created.Result.LedgerRevision || direct.ContentDigest != created.Result.ContentDigest || created.Result.StreamRevision != 1 {
		t.Fatalf("scope retry: %+v, %v", direct, err)
	}
	scope.Definition.Label = "Revised case"
	if _, err := client.PutScope(ctx, scope); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("changed scope retry: %v", err)
	}
	scope.Command = cmd("remote-scope-update")
	if _, err := client.PutScope(ctx, scope); connect.CodeOf(err) != connect.CodeAborted {
		t.Fatalf("scope revision: %v", err)
	}
	scope.ExpectedRevision = 1
	if _, err := client.PutScope(ctx, scope); err != nil {
		t.Fatal(err)
	}
	historical, err := client.GetScope(ctx, &v1.GetScopeRequest{Tenant: c.Tenant, Scope: "remote-case", Purpose: "review", Revision: 1})
	if err != nil || historical.Version.Definition.Label != "Remote case" || historical.Version.Revision != 1 || historical.Version.LedgerRevision != created.Result.LedgerRevision || historical.Version.Caller.Principal != c.Principal || historical.Version.Reason != "Create scope" {
		t.Fatalf("scope history: %v, %v", historical, err)
	}
	current, err := client.GetScope(ctx, &v1.GetScopeRequest{Tenant: c.Tenant, Scope: "remote-case", Purpose: "review"})
	if err != nil || current.Version.Revision != 2 || current.Version.Definition.Label != "Revised case" {
		t.Fatalf("current scope: %v, %v", current, err)
	}
	subject := &v1.PutSubjectRequest{Tenant: c.Tenant, Command: cmd("remote-subject"), Definition: &v1.SubjectDefinition{Scope: "remote-case", Id: "remote-subject", Kind: "customer", Label: "Customer", Lookup: map[string]string{"crm": "C-REMOTE"}}, Reason: "Register customer"}
	if _, err := blocked.PutSubject(ctx, subject); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("subject admin: %v", err)
	}
	first, err := client.PutSubject(ctx, subject)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := e.PutSubject(ctx, c, model.SubjectRequest{Command: model.Command{Key: subject.Command.Key, Purpose: subject.Command.Purpose}, Definition: model.SubjectDefinition{Scope: subject.Definition.Scope, ID: subject.Definition.Id, Kind: subject.Definition.Kind, Label: subject.Definition.Label, Lookup: subject.Definition.Lookup}, Reason: subject.Reason})
	if err != nil || replay.ID != first.Result.Id || replay.LedgerRevision != first.Result.LedgerRevision || replay.ContentDigest != first.Result.ContentDigest {
		t.Fatalf("subject retry: %+v, %v", replay, err)
	}
	lookup := &v1.LookupSubjectRequest{Tenant: c.Tenant, Scope: "remote-case", Namespace: "crm", Value: "C-REMOTE", Purpose: "lookup"}
	found, err := client.LookupSubject(ctx, lookup)
	if err != nil {
		t.Fatal(err)
	}
	read, err := client.GetSubject(ctx, &v1.GetSubjectRequest{Tenant: c.Tenant, Scope: "remote-case", Id: "remote-subject", Purpose: "review"})
	if err != nil || !proto.Equal(found.Version, read.Version) || !proto.Equal(read.Version.Definition, subject.Definition) || read.Version.Caller.Principal != c.Principal || read.Version.Reason != subject.Reason || read.Version.LedgerRevision != first.Result.LedgerRevision {
		t.Fatalf("subject lookup: %v", err)
	}
	subject.Command = cmd("remote-duplicate-alias")
	subject.Definition.Id = "duplicate"
	if _, err := client.PutSubject(ctx, subject); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("duplicate external identifier: %v", err)
	}
	if _, err := client.GetSubject(ctx, &v1.GetSubjectRequest{Tenant: c.Tenant, Scope: "remote-case", Id: "duplicate", Purpose: "review"}); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("rejected subject persisted: %v", err)
	}
	subject.Definition.Id = "remote-subject"
	subject.Command = cmd("remote-subject-update")
	subject.ExpectedRevision = 1
	subject.Definition.Lookup = map[string]string{"crm": "C-NEW"}
	if _, err := client.PutSubject(ctx, subject); err != nil {
		t.Fatal(err)
	}
	if _, err := client.LookupSubject(ctx, lookup); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("old lookup retained: %v", err)
	}
	lookup.Value = "C-NEW"
	updated, err := client.LookupSubject(ctx, lookup)
	if err != nil || updated.Version.Revision != 2 {
		t.Fatalf("updated lookup: %v", err)
	}
	old, err := client.GetSubject(ctx, &v1.GetSubjectRequest{Tenant: c.Tenant, Scope: "remote-case", Id: "remote-subject", Purpose: "review", Revision: 1})
	if err != nil || !proto.Equal(old.Version, read.Version) {
		t.Fatalf("immutable subject history: %v", err)
	}
	lookup.Scope = "case1"
	if _, err := client.LookupSubject(ctx, lookup); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("cross-scope lookup: %v", err)
	}
	subject.Command = cmd("remote-kind-change")
	subject.ExpectedRevision = 2
	subject.Definition.Kind = "account"
	if _, err := client.PutSubject(ctx, subject); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("subject kind mutation: %v", err)
	}
}

func testRemoteIdentityReadDenied(t *testing.T, ctx context.Context, e *facts.Engine, c model.Caller, history bool) {
	t.Helper()
	client := memoryv1connect.NewIdentityServiceClient(governanceClient(t, e, c))
	revision := int64(0)
	if history {
		revision = 1
	}
	if _, err := client.GetScope(ctx, &v1.GetScopeRequest{Tenant: c.Tenant, Scope: "case1", Purpose: "review", Revision: revision}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("scope read permission: %v", err)
	}
	if _, err := client.GetSubject(ctx, &v1.GetSubjectRequest{Tenant: c.Tenant, Scope: "case1", Id: "subject1", Purpose: "review", Revision: revision}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("subject read permission: %v", err)
	}
	if !history {
		if _, err := client.LookupSubject(ctx, &v1.LookupSubjectRequest{Tenant: c.Tenant, Scope: "case1", Namespace: "crm", Value: "C-456", Purpose: "review"}); connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("lookup permission: %v", err)
		}
	}
}
