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
	for _, name := range []string{"00001_ledger.sql", "00002_evidence_objects.sql", "00003_active_definitions.sql", "00004_temporal_snapshots.sql", "00005_calculations_audit.sql", "00006_outbox_consumers.sql", "00007_projection_rebuilds.sql", "00008_context_decisions.sql", "00009_retention.sql", "00010_conversations.sql", "00011_extraction_jobs.sql", "00012_extraction_runs.sql", "00013_predicate_migrations.sql"} {
		if name == "00004_temporal_snapshots.sql" {
			seedLegacyTimeline(t, ctx, admin)
		}
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
	if _, err := admin.Exec(ctx, string(role)); err != nil {
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
	t.Run("fact listing", func(t *testing.T) { testFactListing(t, ctx, ledger, store) })
	t.Run("retention governance", func(t *testing.T) { testRetention(t, ctx, engine, store, admin) })
	t.Run("outbox delivery recovery", func(t *testing.T) { testOutbox(t, ctx, ledger, admin) })
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
	testEvidenceAccess(t, ctx, ledger, store, caller)
	now := time.Now().UTC().Truncate(time.Microsecond)
	value := "tea"
	q := model.FactProposal{Command: cmd("proposal"), Key: model.FactKey{Scope: "case1", Subject: "subject1", Predicate: "preference"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &now}, Sources: []model.SourceSpan{{EvidenceID: "ev1", Start: 9, End: 12, Quote: "tea"}}}
	t.Run("predicate value constraints", func(t *testing.T) {
		maximum := 3
		restricted := def
		restricted.Command = cmd("restricted-schema")
		restricted.Definition.Predicate = "restricted-preference"
		restricted.Definition.Cardinality = "single"
		restricted.Definition.Constraints = &model.ValueConstraints{MaxTextRunes: &maximum, AllowedValues: []model.Value{q.Value}}
		if _, err := engine.RegisterPredicateVersion(ctx, caller, restricted); err != nil {
			t.Fatal(err)
		}
		request := q
		request.Command = cmd("restricted-proposal")
		request.Key.Predicate = restricted.Definition.Predicate
		bad := "coffee"
		request.Value = model.Value{Kind: model.Text, Text: &bad}
		if _, err := engine.ProposeFact(ctx, caller, request); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("schema bypass", err)
		}
		request.Value = q.Value
		accepted, err := engine.ProposeFact(ctx, caller, request)
		if err != nil {
			t.Fatal("invalid request persisted receipt", err)
		}
		if _, err = engine.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("restricted-approval"), Scope: "case1", RecordID: accepted.RecordID, VersionID: accepted.VersionID, ExpectedRevision: accepted.StreamRevision}); err != nil {
			t.Fatal(err)
		}
		if err = ledger.Transact(ctx, caller.Tenant, func(tx model.Tx) error {
			stored, err := tx.Predicate(ctx, "case1", restricted.Definition.Predicate, "v1")
			if err != nil {
				return err
			}
			if model.Digest(stored) != model.Digest(restricted.Definition) {
				t.Fatal("schema constraints not retained")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
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
				var detail *model.RevisionConflict
				if !errors.As(err, &detail) || detail.RecordID != approval.RecordID || detail.ExpectedRevision != approval.ExpectedRevision || detail.CurrentRevision != approval.ExpectedRevision+1 {
					t.Fatal("missing or incorrect locked revision", err, detail)
				}
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
		acceptedAuto, err := engine.AcceptFact(ctx, auto, accept)
		if err != nil {
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
		historicalAuto, err := engine.GetFact(ctx, caller, model.FactQuery{Key: proposalRequest.Key, ValidAt: now, KnowledgeRevision: acceptedAuto.LedgerRevision})
		if err != nil || historicalAuto.State != "resolved" {
			t.Fatal(historicalAuto, err)
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
		newer.Definition.Migration = &model.PredicateMigration{FromVersion: "v1", Mode: "preserve_history_repropose", Reason: "policy_review"}
		missingRule := newer
		missingRule.Definition.Migration = nil
		if _, err := engine.RegisterPredicateVersion(ctx, caller, missingRule); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("implicit schema migration accepted", err)
		}
		if _, err := engine.RegisterPredicateVersion(ctx, caller, newer); err != nil {
			t.Fatal(err)
		}
		// A committed retry retains its result even if a later migration wins.
		migrations := make(chan error, 2)
		for _, version := range []string{"v3", "v4"} {
			go func(version string) {
				next := def
				next.Command = cmd("schema-" + version)
				next.Definition.Version = version
				next.Definition.Migration = &model.PredicateMigration{FromVersion: "v2", Mode: "preserve_history_repropose", Reason: "concurrent_review"}
				_, err := engine.RegisterPredicateVersion(ctx, caller, next)
				migrations <- err
			}(version)
		}
		won, stale := 0, 0
		for i := 0; i < 2; i++ {
			err := <-migrations
			if err == nil {
				won++
			} else if errors.Is(err, model.ErrPrecondition) {
				stale++
			} else {
				t.Fatal(err)
			}
		}
		if won != 1 || stale != 1 {
			t.Fatal("concurrent migrations both activated", won, stale)
		}
		if _, err := engine.RegisterPredicateVersion(ctx, caller, newer); err != nil {
			t.Fatal("committed migration retry failed", err)
		}
		current, err := engine.GetFact(ctx, reviewer, model.FactQuery{Key: q.Key, ValidAt: now})
		if err != nil || current.State != "resolved" || current.VersionID != proposal.VersionID {
			t.Fatal("migration rewrote accepted history", current, err)
		}
		old := q
		old.Command = cmd("old-schema-proposal")
		old.Key.Subject = "subject-new"
		if _, err := engine.ProposeFact(ctx, caller, old); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal(err)
		}
	})

	t.Run("temporal lifecycle and immutable snapshots", func(t *testing.T) {
		definition := def
		definition.Command = cmd("timeline-schema")
		definition.Definition.Predicate = "timeline"
		if _, err := engine.RegisterPredicateVersion(ctx, caller, definition); err != nil {
			t.Fatal(err)
		}
		day := func(month, day int) time.Time { return time.Date(2026, time.Month(month), day, 0, 0, 0, 0, time.UTC) }
		jan1, jan10, jan12, jan15, jan20, feb1, feb10, feb15 := day(1, 1), day(1, 10), day(1, 12), day(1, 15), day(1, 20), day(2, 1), day(2, 10), day(2, 15)
		key := model.FactKey{Scope: "case1", Subject: "temporal-subject", Predicate: "timeline"}
		propose := func(command, value string, valid model.Interval, expected int64) model.CommitResult {
			t.Helper()
			r := q
			r.Command = cmd(command)
			r.Key = key
			r.ExpectedRevision = expected
			r.Valid = valid
			r.Value = model.Value{Kind: model.Text, Text: &value}
			out, err := engine.ProposeFact(ctx, caller, r)
			if err != nil {
				t.Fatal(err)
			}
			return out
		}
		read := func(at time.Time, revision int64, state, value string) {
			t.Helper()
			r, err := engine.GetFact(ctx, reviewer, model.FactQuery{Key: key, ValidAt: at, KnowledgeRevision: revision})
			if err != nil || r.State != state {
				t.Fatalf("read %s at rev %d: %+v %v", at, revision, r, err)
			}
			if value != "" && (r.Value == nil || *r.Value.Text != value) {
				t.Fatalf("wrong value: %+v", r)
			}
			if state == "disputed" || state == "retracted" {
				if r.Value != nil {
					t.Fatal("unusable value disclosed")
				}
			}
		}
		a := propose("timeline-a", "A", model.Interval{From: &jan1}, 0)
		acceptedA, err := engine.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("timeline-approve-a"), Scope: "case1", RecordID: a.RecordID, VersionID: a.VersionID, ExpectedRevision: 1})
		if err != nil {
			t.Fatal(err)
		}
		missing := key
		missing.Subject = "missing-subject"
		request := model.FactSnapshotRequest{Command: cmd("snapshot-a"), Selectors: []model.FactKey{key, missing}, ValidAt: jan15}
		snap, err := engine.CreateFactSnapshot(ctx, reviewer, request)
		if err != nil {
			t.Fatal(err)
		}
		if len(snap.Items) != 2 || snap.Items[0].Resolution.VersionID != a.VersionID || snap.Items[1].Resolution.State != "missing" || snap.SelectionRevision >= snap.CreationRevision {
			t.Fatal(snap)
		}
		calc := recordCalculationFixture(t, ctx, engine, store, caller, snap)
		b := propose("timeline-b", "B", model.Interval{From: &feb1}, 2)
		replacement := model.Supersession{Approval: model.Approval{Command: cmd("replace-b"), Scope: "case1", RecordID: b.RecordID, VersionID: b.VersionID, ExpectedRevision: 3}, ReplacedVersionIDs: []string{a.VersionID}, Valid: model.Interval{From: &feb1}, Reason: "updated_evidence"}
		acceptedB, err := engine.SupersedeFact(ctx, reviewer, replacement)
		if err != nil {
			t.Fatal(err)
		}
		read(jan15, 0, "resolved", "A")
		read(feb15, 0, "resolved", "B")
		read(feb15, acceptedA.LedgerRevision, "resolved", "A")
		cVersion := propose("timeline-c", "C", model.Interval{From: &jan10, To: &feb10}, 4)
		correction := model.Supersession{Approval: model.Approval{Command: cmd("replace-c"), Scope: "case1", RecordID: cVersion.RecordID, VersionID: cVersion.VersionID, ExpectedRevision: 5}, ReplacedVersionIDs: []string{a.VersionID}, Valid: model.Interval{From: &jan10, To: &feb10}, Reason: "backdated_correction"}
		if _, err := engine.SupersedeFact(ctx, reviewer, correction); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("incomplete overlaps", err)
		}
		correction.ReplacedVersionIDs = append(correction.ReplacedVersionIDs, b.VersionID)
		acceptedC, err := engine.SupersedeFact(ctx, reviewer, correction)
		if err != nil {
			t.Fatal(err)
		}
		read(day(1, 9), 0, "resolved", "A")
		read(jan15, 0, "resolved", "C")
		read(feb15, 0, "resolved", "B")
		read(day(2, 5), acceptedB.LedgerRevision, "resolved", "B")
		change := model.LifecycleChange{Approval: model.Approval{Command: cmd("dispute-c"), Scope: "case1", RecordID: cVersion.RecordID, VersionID: cVersion.VersionID, ExpectedRevision: 6}, Valid: model.Interval{From: &jan12, To: &jan20}, Reason: "client_disputed"}
		disputed, err := engine.DisputeFact(ctx, reviewer, change)
		if err != nil {
			t.Fatal(err)
		}
		read(jan15, 0, "disputed", "")
		read(jan15, acceptedC.LedgerRevision, "resolved", "C")
		request2 := request
		request2.Command = cmd("disputed-snapshot")
		disputedSnap, err := engine.CreateFactSnapshot(ctx, reviewer, request2)
		if err != nil || disputedSnap.Items[0].Resolution.State != "disputed" {
			t.Fatal(disputedSnap, err)
		}
		change.Command = cmd("resolve-c")
		change.ExpectedRevision = 7
		if _, err := engine.ResolveDispute(ctx, reviewer, change); err != nil {
			t.Fatal(err)
		}
		read(jan15, 0, "resolved", "C")
		read(jan15, disputed.LedgerRevision, "disputed", "")
		change.Command = cmd("retract-c")
		change.ExpectedRevision = 8
		if _, err := engine.RetractFact(ctx, reviewer, change); err != nil {
			t.Fatal(err)
		}
		read(jan15, 0, "retracted", "")
		// Approval can fill a retracted interval without losing its historical withdrawal.
		d := propose("timeline-d", "D", model.Interval{From: &jan12, To: &jan20}, 9)
		if _, err := engine.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("approve-d"), Scope: "case1", RecordID: d.RecordID, VersionID: d.VersionID, ExpectedRevision: 10}); err != nil {
			t.Fatal(err)
		}
		read(jan15, 0, "resolved", "D")
		oldCalc, err := engine.GetCalculationReceipt(ctx, caller, model.CalculationQuery{ID: calc.ID, Purpose: "audit"})
		if err != nil || oldCalc.SnapshotDigest != snap.Digest || oldCalc.Receipt.SnapshotID != snap.ID {
			t.Fatal(oldCalc, err)
		}
		stored, err := engine.GetFactSnapshot(ctx, reviewer, model.SnapshotQuery{ID: snap.ID, Purpose: "integration"})
		if err != nil || stored.Digest != snap.Digest || *stored.Items[0].Resolution.Value.Text != "A" {
			t.Fatal(stored, err)
		}
		retry, err := engine.CreateFactSnapshot(ctx, reviewer, request)
		if err != nil || retry.ID != snap.ID || retry.Digest != snap.Digest {
			t.Fatal(retry, err)
		}
		changed := request
		changed.ValidAt = feb15
		if _, err := engine.CreateFactSnapshot(ctx, reviewer, changed); !errors.Is(err, model.ErrConflict) {
			t.Fatal(err)
		}
		request2.Command = cmd("historical-snapshot")
		request2.KnowledgeRevision = acceptedA.LedgerRevision
		request2.ValidAt = feb15
		historical, err := engine.CreateFactSnapshot(ctx, reviewer, request2)
		if err != nil || *historical.Items[0].Resolution.Value.Text != "A" {
			t.Fatal(historical, err)
		}
		rejectedVersion := propose("timeline-rejected", "unaccepted", model.Interval{From: &jan1}, 11)
		rejection := model.LifecycleChange{Approval: model.Approval{Command: cmd("reject-proposal"), Scope: "case1", RecordID: rejectedVersion.RecordID, VersionID: rejectedVersion.VersionID, ExpectedRevision: 12}, Reason: "unsupported"}
		if _, err := engine.RejectFact(ctx, reviewer, rejection); err != nil {
			t.Fatal(err)
		}
		attempt := rejection.Approval
		attempt.Command = cmd("accept-rejected")
		attempt.ExpectedRevision = 13
		if _, err := engine.ApproveFact(ctx, reviewer, attempt); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal(err)
		}
		history, err := engine.GetFactHistory(ctx, reviewer, model.HistoryQuery{Scope: "case1", RecordID: a.RecordID, Purpose: "audit"})
		if err != nil || len(history.Events) != 13 {
			t.Fatal(history, err)
		}
		found := false
		for _, event := range history.Events {
			if event.Type == "supersede_fact" && event.Reason == "backdated_correction" && len(event.ReplacedVersionIDs) == 2 && event.Affected != nil {
				found = true
			}
		}
		if !found {
			t.Fatal("missing correction provenance")
		}
	})

	t.Run("snapshot uses one database view", func(t *testing.T) {
		gate := &pauseLedger{Ledger: ledger, read: make(chan struct{}), proceed: make(chan struct{})}
		observer, err := facts.New(gate, allow{}, store)
		if err != nil {
			t.Fatal(err)
		}
		key := model.FactKey{Scope: "case1", Subject: "temporal-subject", Predicate: "timeline"}
		newKey := key
		newKey.Subject = "created-during-snapshot"
		at := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
		request := model.FactSnapshotRequest{Command: cmd("concurrent-snapshot"), Selectors: []model.FactKey{key, newKey}, ValidAt: at}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		type outcome struct {
			snapshot model.FactSnapshot
			err      error
		}
		result := make(chan outcome, 1)
		go func() {
			snapshot, err := observer.CreateFactSnapshot(ctx, reviewer, request)
			result <- outcome{snapshot, err}
		}()
		select {
		case <-gate.read:
		case <-ctx.Done():
			t.Fatal("snapshot failed to reach barrier")
		}
		// Always release the read transaction, including when a mutation assertion fails.
		var release sync.Once
		defer release.Do(func() { close(gate.proceed) })
		proposal := q
		proposal.Command = cmd("during-snapshot-proposal")
		proposal.Key = newKey
		proposal.Valid.From = &at
		pr, err := engine.ProposeFact(ctx, caller, proposal)
		if err != nil {
			t.Fatal(err)
		}
		commit, err := engine.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("during-snapshot-approval"), Scope: "case1", RecordID: pr.RecordID, VersionID: pr.VersionID, ExpectedRevision: 1})
		if err != nil {
			t.Fatal(err)
		}
		release.Do(func() { close(gate.proceed) })
		out := <-result
		if out.err != nil {
			t.Fatal(out.err)
		}
		if out.snapshot.Items[1].Resolution.State != "missing" || out.snapshot.SelectionRevision >= commit.LedgerRevision || out.snapshot.CreationRevision <= commit.LedgerRevision {
			t.Fatal("mixed snapshot boundary", out.snapshot)
		}
		current, err := engine.GetFact(ctx, reviewer, model.FactQuery{Key: newKey, ValidAt: at})
		if err != nil || current.State != "resolved" {
			t.Fatal(current, err)
		}
	})
	t.Run("unauthorized snapshot selector fails", func(t *testing.T) {
		restricted, err := facts.New(ledger, denyScope{scope: "forbidden"}, store)
		if err != nil {
			t.Fatal(err)
		}
		deniedKey := model.FactKey{Scope: "forbidden", Subject: "opaque", Predicate: "timeline"}
		_, err = restricted.CreateFactSnapshot(ctx, caller, model.FactSnapshotRequest{Command: cmd("denied-snapshot"), Selectors: []model.FactKey{q.Key, deniedKey}, ValidAt: now})
		if !errors.Is(err, model.ErrDenied) {
			t.Fatal(err)
		}
	})

	t.Run("upgrade retains earlier timeline generations", func(t *testing.T) {
		var generations, entries int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM timeline_generations WHERE tenant_id='legacy'`).Scan(&generations); err != nil {
			t.Fatal(err)
		}
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM timeline_history WHERE tenant_id='legacy'`).Scan(&entries); err != nil {
			t.Fatal(err)
		}
		if generations != 2 || entries != 3 {
			t.Fatal(generations, entries)
		}
		var original []byte
		if err := admin.QueryRow(ctx, `SELECT payload FROM timeline_history WHERE tenant_id='legacy' AND ledger_revision=1`).Scan(&original); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(original, []byte{1}) {
			t.Fatal("encrypted historical payload changed")
		}
	})

	t.Run("signed audit export and recovery", func(t *testing.T) { testAuditExports(t, ctx, ledger, store, reviewer, admin) })

	t.Run("projection and archive consumers", func(t *testing.T) { testPipeline(t, ctx, ledger, store, admin) })
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
