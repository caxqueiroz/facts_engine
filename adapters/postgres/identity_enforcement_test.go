package postgres_test

import (
	"context"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
	"testing"
)

func registerTestIdentities(t *testing.T, ctx context.Context, e *facts.Engine, tenant, scope string, subjects []string) {
	t.Helper()
	c := model.Caller{Tenant: tenant, Principal: "fixture-administrator"}
	_, err := e.PutScope(ctx, c, model.ScopeRequest{Command: model.Command{Key: "fixture-scope-" + scope, Purpose: "test-setup"}, Definition: model.ScopeDefinition{ID: scope, Label: scope}, Reason: "Explicit integration fixture"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range subjects {
		_, err = e.PutSubject(ctx, c, model.SubjectRequest{Command: model.Command{Key: "fixture-subject-" + scope + "-" + id, Purpose: "test-setup"}, Definition: model.SubjectDefinition{Scope: scope, ID: id, Kind: "test-subject", Label: id}, Reason: "Explicit integration fixture"})
		if err != nil {
			t.Fatal(err)
		}
	}
}
func testIdentityAdoption(t *testing.T, ctx context.Context, l *postgres.Ledger, store *evidence.Postgres, admin *pgxpool.Pool) {
	t.Helper()
	e, err := facts.New(l, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := os.ReadFile("../../scripts/identity-inventory.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, string(inventory)); err != nil {
		t.Fatal("inventory script", err)
	}
	// The old fixture predates the registry. Migration preserves it without
	// manufacturing a label, subject kind, verified ownership, or historical event.
	err = l.Transact(ctx, "legacy", func(tx model.Tx) error { return tx.RequireScope(ctx, "scope") })
	if !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("legacy scope implicitly trusted", err)
	}
	err = l.Transact(ctx, "legacy", func(tx model.Tx) error { return tx.RequireSubject(ctx, "scope", "subject") })
	if !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("legacy subject implicitly trusted", err)
	}
	if _, err = admin.Exec(ctx, `ALTER TABLE records VALIDATE CONSTRAINT records_registered_subject`); err == nil {
		t.Fatal("unadopted history validated")
	}
	var original []byte
	if err = admin.QueryRow(ctx, `SELECT payload FROM ledger_events WHERE tenant_id='legacy' AND id='ev1'`).Scan(&original); err != nil {
		t.Fatal(err)
	}
	registerTestIdentities(t, ctx, e, "legacy", "scope", []string{"subject"})
	err = l.Transact(ctx, "legacy", func(tx model.Tx) error {
		if err := tx.RequireScope(ctx, "scope"); err != nil {
			return err
		}
		return tx.RequireSubject(ctx, "scope", "subject")
	})
	if err != nil {
		t.Fatal("adopted identity unavailable", err)
	}
	var retained []byte
	if err = admin.QueryRow(ctx, `SELECT payload FROM ledger_events WHERE tenant_id='legacy' AND id='ev1'`).Scan(&retained); err != nil || string(retained) != string(original) {
		t.Fatal("history rewritten", err)
	}
	raw, err := os.ReadFile("../../scripts/validate-identities.sql")
	if err != nil {
		t.Fatal(err)
	}
	validation := strings.ReplaceAll(string(raw), "\\set ON_ERROR_STOP on", "")
	if _, err = admin.Exec(ctx, validation); err != nil {
		t.Fatal("adopted constraints", err)
	}
	var count int
	if err = admin.QueryRow(ctx, `SELECT count(*) FROM pg_constraint WHERE conname LIKE '%_registered_scope' OR conname='records_registered_subject'`).Scan(&count); err != nil || count != 11 {
		t.Fatal("constraint inventory", count, err)
	}
	if err = admin.QueryRow(ctx, `SELECT count(*) FROM pg_constraint WHERE (conname LIKE '%_registered_scope' OR conname='records_registered_subject') AND NOT convalidated`).Scan(&count); err != nil || count != 0 {
		t.Fatal("unvalidated adoption", count, err)
	}
	// Even privileged direct SQL cannot add dangling canonical references.
	for _, query := range []string{
		`INSERT INTO records(tenant_id,id,scope_id,subject_id,predicate,discriminator) VALUES('legacy','unknown-subject','scope','absent','predicate','')`,
		`INSERT INTO records(tenant_id,id,scope_id,subject_id,predicate,discriminator) VALUES('other-tenant','foreign-subject','scope','subject','predicate','')`,
		`INSERT INTO ledger_events(tenant_id,id,ledger_revision,event_type,scope_id,payload) VALUES('legacy','unknown-scope',999,'test','absent',decode('01','hex'))`,
	} {
		if _, err = admin.Exec(ctx, query); err == nil {
			t.Fatal("registry FK bypass", query)
		}
	}
	c := model.Caller{Tenant: "legacy", Principal: "fixture-administrator"}
	policy := model.PolicyRequest{Command: model.Command{Key: "unknown-scope-policy", Purpose: "test"}, Policy: model.AcceptancePolicy{Scope: "absent", ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}
	if _, err = e.RegisterAcceptancePolicy(ctx, c, policy); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("unknown scope write", err)
	}
	// An authorized operation cannot turn an unregistered identity into existence.
	err = l.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		_, err := tx.LockRecord(ctx, model.FactKey{Scope: "scope", Subject: "absent", Predicate: "predicate"})
		return err
	})
	if !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("implicit subject creation", err)
	}
}
