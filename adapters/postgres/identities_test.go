package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"sync"
	"testing"
)

func testIdentities(t *testing.T, ctx context.Context, l *postgres.Ledger, store *evidence.Postgres) {
	e, err := facts.New(l, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	c := model.Caller{Tenant: "identity-test", Principal: "administrator"}
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "identity-administration"} }
	scope := model.ScopeRequest{Command: cmd("scope"), Definition: model.ScopeDefinition{ID: "case1", Label: "Case one"}, Reason: "Create case"}
	denied, _ := facts.New(l, denyPermission{permission: "can_manage_identities"}, store)
	if _, err = denied.PutScope(ctx, c, scope); !errors.Is(err, model.ErrDenied) {
		t.Fatal("admin required", err)
	}
	t.Run("remote identities", func(t *testing.T) { testRemoteIdentities(t, ctx, e, denied, c) })
	a, err := e.PutScope(ctx, c, scope)
	if err != nil || a.StreamRevision != 1 {
		t.Fatal(a, err)
	}
	if b, err := e.PutScope(ctx, c, scope); err != nil || b != a {
		t.Fatal("retry", b, err)
	}
	scope.Definition.Label = "Case revised"
	if _, err = e.PutScope(ctx, c, scope); !errors.Is(err, model.ErrConflict) {
		t.Fatal("changed retry", err)
	}
	scope.Command = cmd("scope-update")
	if _, err = e.PutScope(ctx, c, scope); !errors.Is(err, model.ErrRevision) {
		t.Fatal("stale revision", err)
	}
	scope.ExpectedRevision = 1
	if _, err = e.PutScope(ctx, c, scope); err != nil {
		t.Fatal(err)
	}
	q := model.IdentityQuery{Scope: "case1", Purpose: "identity-read", Revision: 1}
	if v, err := e.GetScope(ctx, c, q); err != nil || v.Definition.Label != "Case one" || v.LedgerRevision != a.LedgerRevision {
		t.Fatal("scope history", v, err)
	}
	q.Revision = 0
	if v, err := e.GetScope(ctx, c, q); err != nil || v.Definition.Label != scope.Definition.Label {
		t.Fatal("current scope", v, err)
	}
	subject := model.SubjectRequest{Command: cmd("subject"), Definition: model.SubjectDefinition{Scope: "case1", ID: "subject1", Kind: "customer", Label: "Customer", Lookup: map[string]string{"crm": "C-123"}}, Reason: "Register customer"}
	bad := subject
	bad.Command = cmd("unregistered-scope")
	bad.Definition.Scope = "unknown"
	if _, err = e.PutSubject(ctx, c, bad); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("parent scope", err)
	}
	first, err := e.PutSubject(ctx, c, subject)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := e.PutSubject(ctx, c, subject); err != nil || b != first {
		t.Fatal("subject retry", b, err)
	}
	lookup := model.SubjectLookupQuery{Scope: "case1", Namespace: "crm", Value: "C-123", Purpose: "lookup"}
	if v, err := e.LookupSubject(ctx, c, lookup); err != nil || v.Definition.ID != "subject1" {
		t.Fatal("lookup", v, err)
	}
	bad = subject
	bad.Command = cmd("alias-conflict")
	bad.Definition.ID = "subject2"
	if _, err = e.PutSubject(ctx, c, bad); !errors.Is(err, model.ErrConflict) {
		t.Fatal("alias conflict", err)
	}
	q.ID = "subject2"
	if _, err = e.GetSubject(ctx, c, q); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("failed claim created subject", err)
	}
	bad = subject
	bad.Command = cmd("kind-change")
	bad.ExpectedRevision = 1
	bad.Definition.Kind = "account"
	if _, err = e.PutSubject(ctx, c, bad); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("kind mutation", err)
	}
	subject.Command = cmd("subject-update")
	subject.ExpectedRevision = 1
	subject.Definition.Lookup = map[string]string{"crm": "C-456"}
	update, err := e.PutSubject(ctx, c, subject)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.LookupSubject(ctx, c, lookup); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("old alias active", err)
	}
	q.ID = "subject1"
	q.Revision = 1
	if v, err := e.GetSubject(ctx, c, q); err != nil || v.Definition.Lookup["crm"] != "C-123" {
		t.Fatal("retained identity", v, err)
	}
	denied, _ = facts.New(l, denyPermission{permission: "can_read_history"}, store)
	testRemoteIdentityReadDenied(t, ctx, denied, c, true)
	if _, err = denied.GetSubject(ctx, c, q); !errors.Is(err, model.ErrDenied) {
		t.Fatal("history denied", err)
	}
	denied, _ = facts.New(l, denyPermission{permission: "can_read"}, store)
	testRemoteIdentityReadDenied(t, ctx, denied, c, false)
	if _, err = denied.LookupSubject(ctx, c, lookup); !errors.Is(err, model.ErrDenied) {
		t.Fatal("lookup denied", err)
	}
	other := c
	other.Tenant = "identity-other"
	if _, err = e.GetSubject(ctx, other, q); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("tenant isolation", err)
	}
	if _, err = e.PutScope(ctx, other, model.ScopeRequest{Command: cmd("scope"), Definition: scope.Definition, Reason: "Independent tenant"}); err != nil {
		t.Fatal(err)
	}
	bad = subject
	bad.Command = cmd("other-subject")
	bad.ExpectedRevision = 0
	if _, err = e.PutSubject(ctx, other, bad); err != nil {
		t.Fatal("independent lookup namespace", err)
	}
	// Concurrent claims must produce one owner, with no partially created loser.
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, id := range []string{"racer1", "racer2"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			r := model.SubjectRequest{Command: cmd(id), Definition: model.SubjectDefinition{Scope: "case1", ID: id, Kind: "customer", Label: id, Lookup: map[string]string{"crm": "race"}}, Reason: "Concurrent registration"}
			_, err := e.PutSubject(ctx, c, r)
			results <- err
		}(id)
	}
	close(start)
	wg.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, model.ErrConflict) {
			conflict++
		} else {
			t.Fatal("concurrent claim", err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("ambiguous lookup", success, conflict)
	}
	var event model.Event
	err = l.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		entries, _, err := tx.(model.AuditTx).AuditEntries(ctx, "case1")
		if err != nil {
			return err
		}
		scopes, subjects := 0, 0
		for _, entry := range entries {
			switch entry.Kind {
			case "scope_identity":
				scopes++
			case "subject_identity":
				subjects++
			case "event":
				var candidate model.Event
				if err = json.Unmarshal(entry.Payload, &candidate); err != nil {
					return err
				}
				if candidate.LedgerRevision == update.LedgerRevision {
					event = candidate
				}
			}
		}
		if scopes != 2 || subjects != 3 {
			t.Fatalf("identity audit coverage: scopes=%d subjects=%d", scopes, subjects)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.ID == "" {
		t.Fatal("missing governed identity event")
	}
	archive, err := l.ArchivePayload(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant}, Event: event})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range archive.Entries {
		if entry.Kind == "subject_identity" {
			found = true
		}
	}
	if !found {
		t.Fatal("identity metadata omitted from archive")
	}
}
