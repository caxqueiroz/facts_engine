package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testVersionDisposal(t *testing.T, ctx context.Context, normal *facts.Engine, store *evidence.Postgres, admin, runtime *pgxpool.Pool) {
	cfg := admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["role"] = "facts_retention"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	_, private, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := audit.NewEd25519("version-disposal", private)
	engine, err := facts.NewWithAudit(ledger, allow{}, store, model.AuditOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	c := model.Caller{Tenant: "version-disposal", Principal: "proposer"}
	reviewer := c
	reviewer.Principal = "reviewer"
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "retention"} }
	now := time.Now().UTC().Add(-time.Hour)
	secret := "private retained value"
	for _, scope := range []string{"a", "z"} {
		registerTestIdentities(t, ctx, normal, c.Tenant, scope, []string{"subject"})
		policy := model.AcceptancePolicy{Scope: scope, ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}
		if scope == "a" {
			policy.Required = []model.AttestationRequirement{{Kind: "human_review", Principals: []string{"reviewer"}}}
		}
		if _, err = normal.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: cmd(scope + "-manual"), Policy: policy}); err != nil {
			t.Fatal(err)
		}
		if _, err = normal.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: cmd(scope + "-predicate"), Definition: model.PredicateVersion{Scope: scope, Predicate: "value", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
			t.Fatal(err)
		}
		ref, err := store.Put(ctx, c.Tenant, scope+"-source", []byte(secret))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = normal.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: cmd(scope + "-evidence"), Evidence: model.Evidence{ID: scope + "-source", Scope: scope, SourceSystem: "test", SourceObject: scope, SourceRevision: "v1", Speaker: "customer", SourceClass: "customer", SourceTime: now, MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "source-policy", Object: ref}}); err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"fact", "memory", "snapshot", "attestation"} {
			for _, young := range []bool{false, true} {
				p := model.RetentionPolicy{ID: kind, Scope: scope, RecordClass: kind + "_payload", Purpose: "retention", Jurisdiction: "SG"}
				if young {
					p.ID += "-young"
					p.MinimumAgeSeconds = 3600
				}
				if _, err = engine.RegisterRetentionPolicy(ctx, c, model.RetentionPolicyRequest{Command: cmd(scope + "-" + p.ID), Policy: p}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	fact, err := normal.ProposeFact(ctx, c, model.FactProposal{Command: cmd("fact"), Key: model.FactKey{Scope: "a", Subject: "subject", Predicate: "value"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &secret}, Valid: model.Interval{From: &now}, Sources: []model.SourceSpan{{EvidenceID: "a-source", Start: 0, End: len(secret), Quote: secret}}})
	if err != nil {
		t.Fatal(err)
	}
	attestationID := "review-fact"
	attestationExpiry := time.Now().UTC().Add(time.Hour)
	if _, err = normal.RecordAttestation(ctx, reviewer, model.AttestationRequest{Command: cmd("fact-review"), Attestation: model.Attestation{ID: attestationID, Scope: "a", RecordID: fact.RecordID, VersionID: fact.VersionID, ContentDigest: fact.ContentDigest, Kind: "human_review", EvidenceID: "a-source", Attestor: reviewer, ExpiresAt: &attestationExpiry}}); err != nil {
		t.Fatal("record review attestation", err)
	}
	accepted, err := normal.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("approve"), Scope: "a", RecordID: fact.RecordID, VersionID: fact.VersionID, ExpectedRevision: fact.StreamRevision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = normal.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: cmd("memory-type"), Definition: model.MemoryType{Scope: "z", Subtype: "summary", Version: "v1", PolicyID: "manual", Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	memory, err := normal.ProposeMemory(ctx, c, model.MemoryProposal{Command: cmd("memory"), Key: model.MemoryKey{Scope: "z", ID: "summary"}, Subtype: "summary", SchemaVersion: "v1", Body: secret, Valid: model.Interval{From: &now}, Sources: []model.SourceSpan{{EvidenceID: "z-source", Start: 0, End: len(secret), Quote: secret}}, Dependencies: []model.MemoryDependency{{Scope: "a", RecordID: fact.RecordID, VersionID: fact.VersionID, ContentDigest: fact.ContentDigest, Label: "assertion"}}})
	if err != nil {
		t.Fatal(err)
	}
	request := func(kind, scope string, v model.CommitResult) model.VersionDisposalRequest {
		r := model.VersionDisposalRequest{Command: cmd("dispose-" + kind), Kind: kind, Scope: scope, RecordID: v.RecordID, VersionID: v.VersionID, Reason: "expired", Bindings: []model.VersionRetentionBinding{{Scope: scope, PolicyID: kind, Jurisdiction: "SG"}}}
		if kind == "memory" {
			r.Bindings = append(r.Bindings, model.VersionRetentionBinding{Scope: "a", PolicyID: kind, Jurisdiction: "SG"})
		}
		return r
	}
	testVersionBatch(t, ctx, normal, engine, admin, c, now, secret)
	f, m := request("fact", "a", fact), request("memory", "z", memory)
	cleanupIndex := setupVersionCleanup(t, ctx, admin, c, fact, memory, secret)
	expectedTargets := 1
	if cleanupIndex != nil {
		expectedTargets = 2
	}
	snapshot, err := normal.CreateFactSnapshot(ctx, c, model.FactSnapshotRequest{Command: cmd("snapshot"), Selectors: []model.FactKey{{Scope: "a", Subject: "subject", Predicate: "value"}}, ValidAt: now})
	if err != nil || snapshot.Items[0].Resolution.Value == nil {
		t.Fatal("snapshot must retain the fact value", snapshot, err)
	}
	reject := func(e *facts.Engine, r model.VersionDisposalRequest, want error) {
		t.Helper()
		if _, err := e.DisposeVersion(ctx, c, r); !errors.Is(err, want) {
			t.Fatalf("version disposal got %v want %v", err, want)
		}
	}
	reject(normal, m, model.ErrDenied)
	reject(engine, f, model.ErrPrecondition) // Live memory is a retained derivative.
	bad := m
	bad.Bindings = append([]model.VersionRetentionBinding(nil), m.Bindings[:1]...)
	reject(engine, bad, model.ErrDenied)
	bad = f
	bad.Scope = "z"
	reject(engine, bad, model.ErrDenied)
	bad = f
	bad.VersionID = "unknown"
	reject(engine, bad, model.ErrDenied)
	bad = m
	bad.Bindings = append([]model.VersionRetentionBinding(nil), m.Bindings...)
	bad.Bindings[0].PolicyID = "memory-young"
	reject(engine, bad, model.ErrPrecondition)
	bad = m
	bad.Bindings = append([]model.VersionRetentionBinding(nil), m.Bindings...)
	bad.Bindings[0].PolicyID = "fact"
	reject(engine, bad, model.ErrPrecondition)
	bad = m
	bad.Purpose = "other"
	reject(engine, bad, model.ErrPrecondition)
	bad = m
	bad.Bindings = append([]model.VersionRetentionBinding(nil), m.Bindings...)
	bad.Bindings[0].Jurisdiction = "US"
	reject(engine, bad, model.ErrPrecondition)
	denied, err := facts.New(ledger, denyScope{scope: "a"}, store)
	if err != nil {
		t.Fatal(err)
	}
	reject(denied, m, model.ErrDenied)
	if _, err = engine.PlaceLegalHold(ctx, c, model.LegalHoldRequest{Command: cmd("hold"), ID: "hold", Scope: "a", Reason: "review"}); err != nil {
		t.Fatal(err)
	}
	reject(engine, m, model.ErrPrecondition)
	if _, err = engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: cmd("release"), ID: "hold", Scope: "a", Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,$2,3)", c.Tenant, model.ProjectionConsumer("static")); err != nil {
		t.Fatal(err)
	}
	reject(engine, m, model.ErrPrecondition)
	if _, err = admin.Exec(ctx, "INSERT INTO projection_destinations(tenant_id,generation,consumer_id) VALUES($1,'static',$2)", c.Tenant, model.ProjectionConsumer("static")); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO projection_generations(tenant_id,generation,state,fence) VALUES($1,'static','paused',1)", c.Tenant); err != nil {
		t.Fatal(err)
	}
	reject(engine, m, model.ErrPrecondition)
	if _, err = admin.Exec(ctx, "UPDATE projection_generations SET state='building' WHERE tenant_id=$1 AND generation='static'", c.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "REVOKE DELETE ON memory_version_payloads FROM facts_retention"); err != nil {
		t.Fatal(err)
	}
	reject(engine, m, model.ErrDenied)
	if _, err = admin.Exec(ctx, "GRANT DELETE ON memory_version_payloads TO facts_retention"); err != nil {
		t.Fatal(err)
	}
	if _, err = ledger.ProjectionCleanupStatus(ctx, model.ProjectionCleanupKey{Tenant: c.Tenant, Kind: "memory", VersionID: memory.VersionID}); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("rollback left cleanup obligations", err)
	}
	if got, err := normal.GetMemoryVersion(ctx, c, model.MemoryVersionQuery{Scope: "z", RecordID: memory.RecordID, VersionID: memory.VersionID, Purpose: "read"}); err != nil || got.Proposal.Body != secret {
		t.Fatal("rollback lost memory", got, err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,'archive-pending',3)", c.Tenant); err != nil {
		t.Fatal(err)
	}
	reject(engine, m, model.ErrPrecondition)
	if _, err = admin.Exec(ctx, "DELETE FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id='archive-pending'", c.Tenant); err != nil {
		t.Fatal(err)
	}
	replayArchives := archiveRevisionsBeforeDisposal(t, ctx, admin, cipher, signer, map[string]ed25519.PublicKey{"version-disposal": private.Public().(ed25519.PublicKey)}, c.Tenant, fact.LedgerRevision, accepted.LedgerRevision, memory.LedgerRevision)
	// A lifecycle writer that read the old version must retry after disposal.
	stale, err := runtime.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	if err = ledgerdb.New(stale).BindTenant(ctx, c.Tenant); err != nil {
		t.Fatal(err)
	}
	var revision int64
	if err = stale.QueryRow(ctx, "SELECT stream_revision FROM records WHERE tenant_id=$1 AND id=$2", c.Tenant, memory.RecordID).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	first, err := engine.DisposeVersion(ctx, c, m)
	if err != nil || first.StreamRevision != memory.StreamRevision+1 {
		t.Fatal(first, err)
	}
	_, err = ledgerdb.New(stale).LockRecord(ctx, ledgerdb.LockRecordParams{TenantID: c.Tenant, ID: memory.RecordID})
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "40001" {
		t.Fatal("stale lifecycle writer survived disposal", err)
	}
	if err = stale.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	m.Bindings[0], m.Bindings[1] = m.Bindings[1], m.Bindings[0]
	testVersionDisposalRemote(t, ctx, engine, c, m, first)
	reject(engine, f, model.ErrPrecondition) // The snapshot still retains a copy after memory disposal.
	if _, err = engine.DisposeSnapshot(ctx, c, model.SnapshotDisposalRequest{Command: cmd("dispose-snapshot"), SnapshotID: snapshot.ID, Reason: "expired", Bindings: []model.SnapshotRetentionBinding{{Scope: "a", PolicyID: "snapshot", Jurisdiction: "SG"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = engine.DisposeVersion(ctx, c, f); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("live attestation did not block version disposal", err)
	}
	attestation := model.AttestationDisposalRequest{Kind: "fact", Scope: "a", RecordID: fact.RecordID, VersionID: fact.VersionID, AttestationID: attestationID, PolicyID: "attestation", Jurisdiction: "SG", Purpose: "retention"}
	batchVersion := f
	batchVersion.Command = model.Command{}
	batchVersion.Reason = ""
	batchRequest := model.DisposalBatchRequest{Command: cmd("dispose-fact-batch"), Scope: "a", Reason: "expired", Attestations: []model.AttestationDisposalRequest{attestation}, Versions: []model.VersionDisposalRequest{batchVersion}}
	disposedFact, err := engine.DisposeBatch(ctx, c, batchRequest)
	var finalStreamRevision int64
	if err == nil {
		err = admin.QueryRow(ctx, `SELECT stream_revision FROM records WHERE tenant_id=$1 AND id=$2`, c.Tenant, fact.RecordID).Scan(&finalStreamRevision)
	}
	if err != nil || finalStreamRevision != accepted.StreamRevision+2 {
		t.Fatal(disposedFact, err)
	}
	testDisposalBatchRemote(t, ctx, engine, c, batchRequest, disposedFact)
	replayArchives()
	for _, r := range []model.VersionDisposalRequest{f, m} {
		s, err := ledger.ProjectionCleanupStatus(ctx, model.ProjectionCleanupKey{Tenant: c.Tenant, Kind: r.Kind, VersionID: r.VersionID})
		if err != nil || s.Complete || len(s.Targets) != expectedTargets || s.Targets[0].Generation != "static" {
			t.Fatal("cleanup incorrectly complete", s, err)
		}
		r.Key = "new-" + r.Kind
		reject(engine, r, model.ErrPrecondition)
	}
	if _, err = normal.GetFact(ctx, c, model.FactQuery{Key: model.FactKey{Scope: "a", Subject: "subject", Predicate: "value"}, ValidAt: now, Purpose: "read"}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("fact disclosed after disposal", err)
	}
	if _, err = normal.GetMemoryVersion(ctx, c, model.MemoryVersionQuery{Scope: "z", RecordID: memory.RecordID, VersionID: memory.VersionID, Purpose: "read"}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("memory disclosed after disposal", err)
	}
	export, err := engine.StartAuditExport(ctx, c, model.AuditExportRequest{Command: cmd("audit"), Scope: "a", IncludeEvidence: true})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := engine.RunAuditExport(ctx, c, model.AuditExportQuery{ID: export.ID, Scope: "a", Purpose: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, gap := range bundle.Bundle.Manifest.Gaps {
		found = found || (gap.Kind == "version_content" && gap.ID == fact.VersionID)
	}
	if !found {
		t.Fatal("missing version disposal gap")
	}
	if err = audit.Verify(*bundle.Bundle, map[string]ed25519.PublicKey{"version-disposal": private.Public().(ed25519.PublicKey)}); err != nil {
		t.Fatal(err)
	}
	if cleanupIndex != nil {
		testVersionCleanupExecution(t, ctx, ledger, store, admin, c, cleanupIndex, f, m, disposedFact, first)
	}
}
