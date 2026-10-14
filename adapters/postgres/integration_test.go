package postgres_test

import (
	"bytes"
	"context"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type allow struct{}

func (allow) Check(_ context.Context, r model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	return model.AuthorizationDecision{Allowed: true, ModelID: "test-model", Consistency: "HIGHER_CONSISTENCY", CheckedAt: time.Now()}, nil
}
func TestPostgresLedger(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to a disposable PostgreSQL database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	// This test owns a disposable database; refuse to reset a populated one.
	var exists bool
	if err := admin.QueryRow(ctx, `SELECT to_regclass('public.ledger_events') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("TEST_DATABASE_URL must point to a fresh disposable database")
	}
	for _, name := range []string{"00001_ledger.sql", "00002_evidence_objects.sql", "00003_active_definitions.sql"} {
		b, err := os.ReadFile(filepath.Join("../../sql/migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		up := strings.Split(string(b), "-- +goose Down")[0]
		if _, err := admin.Exec(ctx, up); err != nil {
			t.Fatalf("migration: %v", err)
		}
	}
	role, err := os.ReadFile("../../scripts/runtime-role.sql")
	if err != nil {
		t.Fatal(err)
	}
	roleSQL := strings.Replace(string(role), "CREATE ROLE facts_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS;", "", 1)
	if _, err := admin.Exec(ctx, `DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='facts_runtime') THEN CREATE ROLE facts_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;`+roleSQL); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["role"] = "facts_runtime"
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	store, _ := evidence.NewPostgres(runtime, cipher)
	engine, err := facts.New(ledger, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	caller := model.Caller{Tenant: "bank1", Principal: "proposer"}
	reviewer := model.Caller{Tenant: "bank1", Principal: "reviewer"}
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "integration"} }
	p := model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{Scope: "case1", ID: "manual1", Mode: "manual", SourceClasses: []string{"customer"}}}
	if _, err := engine.RegisterAcceptancePolicy(ctx, caller, p); err != nil {
		t.Fatal(err)
	}
	def := model.PredicateRequest{Command: cmd("schema"), Definition: model.PredicateVersion{Scope: "case1", Predicate: "preference", Version: "v1", PolicyID: "manual1", Kind: model.Text, Classification: "confidential"}}
	if _, err := engine.RegisterPredicateVersion(ctx, caller, def); err != nil {
		t.Fatal(err)
	}
	payload := []byte("I prefer tea.")
	ref, err := store.Put(ctx, caller.Tenant, "message1", payload)
	if err != nil {
		t.Fatal(err)
	}
	e := model.EvidenceRequest{Command: cmd("evidence"), Evidence: model.Evidence{ID: "ev1", Scope: "case1", SourceSystem: "chat", SourceObject: "msg1", SourceRevision: "1", Speaker: "customer", SourceClass: "customer", MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "test", SourceTime: time.Now().UTC(), Object: ref}}
	if _, err := engine.RegisterEvidence(ctx, caller, e); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	value := "tea"
	q := model.FactProposal{Command: cmd("proposal"), Key: model.FactKey{Scope: "case1", Subject: "subject1", Predicate: "preference"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &now}, Sources: []model.SourceSpan{{EvidenceID: "ev1", Start: 9, End: 12, Quote: "tea"}}}
	proposal, err := engine.ProposeFact(ctx, caller, q)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("idempotent response loss", func(t *testing.T) {
		retry, err := engine.ProposeFact(ctx, caller, q)
		if err != nil || retry != proposal {
			t.Fatal(retry, err)
		}
		changed := q
		changed.ExpectedRevision = 1
		if _, err := engine.ProposeFact(ctx, caller, changed); !errors.Is(err, model.ErrConflict) {
			t.Fatal(err)
		}
	})
	approval := model.Approval{Command: cmd("approval"), Scope: "case1", RecordID: proposal.RecordID, VersionID: proposal.VersionID, ExpectedRevision: 1}
	t.Run("concurrent duplicate commands", func(t *testing.T) {
		request := q
		request.Command = cmd("duplicate-race")
		request.Key.Subject = "subject-race"
		type outcome struct {
			result model.CommitResult
			err    error
		}
		results := make(chan outcome, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, err := engine.ProposeFact(ctx, caller, request)
				results <- outcome{r, err}
			}()
		}
		wg.Wait()
		close(results)
		var prior *model.CommitResult
		for out := range results {
			if out.err != nil {
				t.Fatal(out.err)
			}
			if prior != nil && *prior != out.result {
				t.Fatal("duplicate returned different result")
			}
			r := out.result
			prior = &r
		}
	})

	t.Run("separation", func(t *testing.T) {
		if _, err := engine.ApproveFact(ctx, caller, approval); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal(err)
		}
		delegated := model.Caller{Tenant: "bank1", Principal: "delegate", Initiator: "proposer", Service: true}
		if _, err := engine.ApproveFact(ctx, delegated, approval); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal(err)
		}
	})
	t.Run("concurrent approvals", func(t *testing.T) {
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for _, key := range []string{"approval-a", "approval-b"} {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				r := approval
				r.Key = key
				_, err := engine.ApproveFact(ctx, reviewer, r)
				errs <- err
			}(key)
		}
		wg.Wait()
		close(errs)
		success, conflict := 0, 0
		for err := range errs {
			if err == nil {
				success++
			} else if errors.Is(err, model.ErrRevision) {
				conflict++
			} else {
				t.Fatal(err)
			}
		}
		if success != 1 || conflict != 1 {
			t.Fatal(success, conflict)
		}
	})
	t.Run("current value", func(t *testing.T) {
		r, err := engine.GetFact(ctx, reviewer, model.FactQuery{Key: q.Key, ValidAt: now})
		if err != nil || r.State != "resolved" || *r.Value.Text != "tea" {
			t.Fatal(r, err)
		}
	})
	t.Run("cross tenant", func(t *testing.T) {
		other := reviewer
		other.Tenant = "bank2"
		r, err := engine.GetFact(ctx, other, model.FactQuery{Key: q.Key, ValidAt: now})
		if err != nil || r.State != "missing" {
			t.Fatal(r, err)
		}
		if _, err := store.Read(ctx, "bank2", ref); err == nil {
			t.Fatal("evidence leaked")
		}
		if err := ledger.Transact(ctx, "bank2", func(tx model.Tx) error { _, err := tx.Version(ctx, proposal.RecordID, proposal.VersionID); return err }); !errors.Is(err, model.ErrNotFound) {
			t.Fatal(err)
		}
	})
	t.Run("RLS and privileges", func(t *testing.T) {
		conn, err := runtime.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM record_versions`).Scan(&n); err != nil || n != 0 {
			t.Fatal(n, err)
		}
		if _, err := conn.Exec(ctx, `UPDATE record_versions SET digest='tampered'`); err == nil {
			t.Fatal("mutable versions")
		}
		if _, err := conn.Exec(ctx, `TRUNCATE record_versions CASCADE`); err == nil {
			t.Fatal("truncate allowed")
		}
	})
	t.Run("database cross-tenant foreign keys", func(t *testing.T) {
		conn, err := runtime.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id','bank2',true)`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO version_evidence(tenant_id,version_id,evidence_id,scope_id) VALUES('bank2',$1,'ev1','case1')`, proposal.VersionID); err == nil {
			t.Fatal("cross-tenant reference allowed")
		}
	})

	t.Run("invalid span", func(t *testing.T) {
		bad := q
		bad.Key = model.FactKey{Scope: "case1", Subject: "subject2", Predicate: "preference"}
		bad.Command = cmd("badspan")
		bad.Sources = []model.SourceSpan{{EvidenceID: "ev1", Start: 9, End: 13, Quote: "tea"}}
		if _, err := engine.ProposeFact(ctx, caller, bad); !errors.Is(err, model.ErrInvalid) {
			t.Fatal(err)
		}
	})
	t.Run("changed source revision", func(t *testing.T) {
		newRef, err := store.Put(ctx, caller.Tenant, "message2", []byte("changed"))
		if err != nil {
			t.Fatal(err)
		}
		bad := e
		bad.Key = "changed-source"
		bad.Evidence.ID = "ev2"
		bad.Evidence.Object = newRef
		if _, err := engine.RegisterEvidence(ctx, caller, bad); !errors.Is(err, model.ErrConflict) {
			t.Fatal(err)
		}
	})
	t.Run("no overlap", func(t *testing.T) {
		next := q
		next.Command = cmd("proposal2")
		next.ExpectedRevision = 2
		pr, err := engine.ProposeFact(ctx, caller, next)
		if err != nil {
			t.Fatal(err)
		}
		a := approval
		a.Command = cmd("overlap")
		a.VersionID = pr.VersionID
		a.ExpectedRevision = 3
		if _, err := engine.ApproveFact(ctx, reviewer, a); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal(err)
		}
	})
	t.Run("automated attestations and withdrawal", func(t *testing.T) {
		auto := model.Caller{Tenant: "bank1", Principal: "acceptor", Service: true}
		policy := model.PolicyRequest{Command: cmd("auto-policy"), Policy: model.AcceptancePolicy{Scope: "case1", ID: "auto1", Mode: "automated", SourceClasses: []string{"customer"}, AcceptingServices: []string{"acceptor"}, Required: []model.AttestationRequirement{{Kind: "client_confirmation", Principals: []string{"proposer"}}}}}
		if _, err := engine.RegisterAcceptancePolicy(ctx, caller, policy); err != nil {
			t.Fatal(err)
		}
		definition := def
		definition.Command = cmd("auto-schema")
		definition.Definition.Predicate = "confirmed_preference"
		definition.Definition.PolicyID = "auto1"
		if _, err := engine.RegisterPredicateVersion(ctx, caller, definition); err != nil {
			t.Fatal(err)
		}
		proposalRequest := q
		proposalRequest.Command = cmd("auto-proposal")
		proposalRequest.Key.Predicate = "confirmed_preference"
		pr, err := engine.ProposeFact(ctx, caller, proposalRequest)
		if err != nil {
			t.Fatal(err)
		}
		accept := model.Approval{Command: cmd("auto-accept"), Scope: "case1", RecordID: pr.RecordID, VersionID: pr.VersionID, ExpectedRevision: 1}
		if _, err := engine.AcceptFact(ctx, auto, accept); !errors.Is(err, model.ErrPrecondition) {
			t.Fatalf("accepted without confirmation: %v", err)
		}
		att := model.AttestationRequest{Command: cmd("confirm"), Attestation: model.Attestation{ID: "att1", Scope: "case1", RecordID: pr.RecordID, VersionID: pr.VersionID, ContentDigest: pr.ContentDigest, Kind: "client_confirmation", EvidenceID: "ev1", Attestor: caller}}
		wrong := att
		wrong.Attestation.ContentDigest = "wrong"
		if _, err := engine.RecordAttestation(ctx, caller, wrong); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal(err)
		}
		if _, err := engine.RecordAttestation(ctx, caller, att); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.AcceptFact(ctx, auto, accept); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.WithdrawAttestation(ctx, caller, model.Withdrawal{Command: cmd("wrong-scope-withdraw"), Scope: "case2", AttestationID: "att1"}); !errors.Is(err, model.ErrDenied) {
			t.Fatal(err)
		}
		if _, err := engine.WithdrawAttestation(ctx, caller, model.Withdrawal{Command: cmd("withdraw"), Scope: "case1", AttestationID: "att1"}); err != nil {
			t.Fatal(err)
		}
		result, err := engine.GetFact(ctx, caller, model.FactQuery{Key: proposalRequest.Key, ValidAt: now})
		if err != nil || result.State != "ineligible" || result.Value != nil {
			t.Fatal(result, err)
		}
		second := proposalRequest
		second.Command = cmd("auto-proposal-2")
		second.ExpectedRevision = 3
		pr2, err := engine.ProposeFact(ctx, caller, second)
		if err != nil {
			t.Fatal(err)
		}
		// A confirmation digest from A cannot bind to B even when its value is identical.
		att.Command = cmd("confirm-b")
		att.Attestation.ID = "att2"
		att.Attestation.VersionID = pr2.VersionID
		att.Attestation.ContentDigest = pr.ContentDigest
		att.Attestation.ExpiresAt = func() *time.Time { x := time.Now().Add(-time.Second); return &x }()
		if _, err := engine.RecordAttestation(ctx, caller, att); !errors.Is(err, model.ErrInvalid) {
			t.Fatal(err)
		}
	})
	t.Run("inactive schema cannot weaken policy", func(t *testing.T) {
		newer := def
		newer.Command = cmd("schema-v2")
		newer.Definition.Version = "v2"
		if _, err := engine.RegisterPredicateVersion(ctx, caller, newer); err != nil {
			t.Fatal(err)
		}
		old := q
		old.Command = cmd("old-schema-proposal")
		old.Key.Subject = "subject-new"
		if _, err := engine.ProposeFact(ctx, caller, old); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal(err)
		}
	})

	t.Run("atomic ordering", func(t *testing.T) {
		var count, maxRev int64
		if err := admin.QueryRow(ctx, `SELECT count(*),max(ledger_revision) FROM ledger_events WHERE tenant_id='bank1'`).Scan(&count, &maxRev); err != nil || count != maxRev {
			t.Fatal(count, maxRev, err)
		}
		var outbox int64
		admin.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE tenant_id='bank1'`).Scan(&outbox)
		if outbox != count {
			t.Fatal("lost outbox event")
		}
	})
}
