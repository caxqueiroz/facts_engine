package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
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

// Owner SQL simulates the storage transition only. No disposal authorization is
// implied; the public policy/hold/coordinated-cleanup command is still pending.
func testVersionDisposalStorage(t *testing.T, ctx context.Context, normal *facts.Engine, store *evidence.Postgres, admin, runtime *pgxpool.Pool) {
	c := model.Caller{Tenant: "version-retention-storage", Principal: "proposer"}
	command := func(key string) model.Command { return model.Command{Key: key, Purpose: "retention-test"} }
	registerTestIdentities(t, ctx, normal, c.Tenant, "scope", []string{"subject"})
	if _, err := normal.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: command("policy"), Policy: model.AcceptancePolicy{Scope: "scope", ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := normal.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: command("predicate"), Definition: model.PredicateVersion{Scope: "scope", Predicate: "value", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	secret := "private version content"
	ref, err := store.Put(ctx, c.Tenant, "source", []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := normal.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: command("evidence"), Evidence: model.Evidence{ID: "source", Scope: "scope", SourceSystem: "test", SourceObject: "source", SourceRevision: "v1", Speaker: "customer", SourceClass: "customer", SourceTime: now, MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "source-policy", Object: ref}}); err != nil {
		t.Fatal(err)
	}
	source := []model.SourceSpan{{EvidenceID: "source", Start: 0, End: len(secret), Quote: secret}}
	f, err := normal.ProposeFact(ctx, c, model.FactProposal{Command: command("fact"), Key: model.FactKey{Scope: "scope", Subject: "subject", Predicate: "value"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &secret}, Valid: model.Interval{From: &now}, Sources: source})
	if err != nil {
		t.Fatal(err)
	}
	reviewer := c
	reviewer.Principal = "reviewer"
	accepted, err := normal.ApproveFact(ctx, reviewer, model.Approval{Command: command("approve"), Scope: "scope", RecordID: f.RecordID, VersionID: f.VersionID, ExpectedRevision: f.StreamRevision})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := normal.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: command("memory-type"), Definition: model.MemoryType{Scope: "scope", Subtype: "episode", Version: "v1", PolicyID: "manual", Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	m, err := normal.ProposeMemory(ctx, c, model.MemoryProposal{Command: command("memory"), Key: model.MemoryKey{Scope: "scope", ID: "summary"}, Subtype: "episode", SchemaVersion: "v1", Body: secret, Valid: model.Interval{From: &now}, Sources: source, Dependencies: []model.MemoryDependency{{Scope: "scope", RecordID: f.RecordID, VersionID: f.VersionID, ContentDigest: f.ContentDigest, Label: "assertion"}}})
	if err != nil {
		t.Fatal(err)
	}
	memoryAccepted, err := normal.ApproveMemory(ctx, reviewer, model.Approval{Command: command("approve-memory"), Scope: "scope", RecordID: m.RecordID, VersionID: m.VersionID, ExpectedRevision: m.StreamRevision})
	if err != nil {
		t.Fatal(err)
	}
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := audit.NewEd25519("version-key", private)
	replay := archiveRevisionsBeforeDisposal(t, ctx, admin, cipher, signer, map[string]ed25519.PublicKey{"version-key": public}, c.Tenant, f.LedgerRevision, accepted.LedgerRevision, m.LedgerRevision)
	var fv model.Version
	var mv model.MemoryVersion
	if err := ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		var err error
		fv, err = tx.Version(ctx, f.RecordID, f.VersionID)
		if err != nil {
			return err
		}
		mv, err = tx.(model.MemoryTx).MemoryVersion(ctx, m.RecordID, m.VersionID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		kind, id            string
		original, tombstone any
	}{
		{"fact", f.VersionID, fv, model.VersionTombstone(fv)},
		{"memory", m.VersionID, mv, model.MemoryVersionTombstone(mv)},
	} {
		t.Run(item.kind, func(t *testing.T) {
			table := item.kind + "_version_disposals"
			payloads := item.kind + "_version_payloads"
			raw, _ := json.Marshal(item.tombstone)
			sealed, err := cipher.Seal(c.Tenant, raw)
			if err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{
				fmt.Sprintf("INSERT INTO %s(tenant_id,version_id,payload) VALUES($1,$2,$3)", table),
				fmt.Sprintf("DELETE FROM %s WHERE tenant_id=$1 AND version_id=$2 AND payload=$3", payloads),
			} {
				_, err := runtime.Exec(ctx, query, c.Tenant, item.id, sealed)
				var pg *pgconn.PgError
				if !errors.As(err, &pg) || pg.Code != "42501" {
					t.Fatal("runtime disposal permitted", err)
				}
			}
			// A rollback leaves the complete original content and no fence.
			tx, err := admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s(tenant_id,version_id,payload) VALUES($1,$2,$3)", table), c.Tenant, item.id, sealed); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE tenant_id=$1 AND version_id=$2", payloads), c.Tenant, item.id); err != nil {
				t.Fatal(err)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := admin.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE tenant_id=$1 AND version_id=$2", table), c.Tenant, item.id).Scan(&count); err != nil || count != 0 {
				t.Fatal("rollback left tombstone", err)
			}
			// Establish a repeatable-read reference writer before disposal commits.
			stale, err := runtime.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
			if err != nil {
				t.Fatal(err)
			}
			defer stale.Rollback(ctx)
			if err := ledgerdb.New(stale).BindTenant(ctx, c.Tenant); err != nil {
				t.Fatal(err)
			}
			if err := stale.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE tenant_id=$1 AND version_id=$2", payloads), c.Tenant, item.id).Scan(&count); err != nil || count != 1 {
				t.Fatal("stale writer lacks original content", count, err)
			}
			tx, err = admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s(tenant_id,version_id,payload) VALUES($1,$2,$3)", table), c.Tenant, item.id, sealed); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE tenant_id=$1 AND version_id=$2", payloads), c.Tenant, item.id); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			referenceTable := "fact_snapshot_items"
			if item.kind == "memory" {
				referenceTable = "context_memory_items"
			}
			_, err = stale.Exec(ctx, "INSERT INTO "+referenceTable+"(tenant_id,version_id) VALUES($1,$2)", c.Tenant, item.id)
			var staleError *pgconn.PgError
			if !errors.As(err, &staleError) || staleError.Code != "40001" {
				t.Fatal("stale snapshot bypassed disposal", err)
			}
			if err := stale.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			testDisposedVersionReferences(t, ctx, runtime, c.Tenant, item.kind, item.id)
			original, _ := json.Marshal(item.original)
			oldBytes, err := cipher.Seal(c.Tenant, original)
			if err != nil {
				t.Fatal(err)
			}
			_, err = admin.Exec(ctx, fmt.Sprintf("INSERT INTO %s(tenant_id,version_id,payload) VALUES($1,$2,$3)", payloads), c.Tenant, item.id, oldBytes)
			var pg *pgconn.PgError
			if !errors.As(err, &pg) || pg.Code != "23514" {
				t.Fatal("resurrection allowed", err)
			}
			for _, query := range []string{"UPDATE " + table + " SET payload=decode('00','hex')", "DELETE FROM " + table, "TRUNCATE " + table} {
				_, err := runtime.Exec(ctx, query)
				if !errors.As(err, &pg) || pg.Code != "42501" {
					t.Fatal("runtime mutated tombstone", err)
				}
			}
			rtx, err := runtime.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := ledgerdb.New(rtx).BindTenant(ctx, "other"); err != nil {
				t.Fatal(err)
			}
			if err := rtx.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
				t.Fatal("tenant leak", err)
			}
			if err := rtx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, memory := range []bool{false, true} {
		err := ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
			if memory {
				_, err := tx.(model.MemoryTx).MemoryVersion(ctx, m.RecordID, m.VersionID)
				return err
			}
			_, err := tx.Version(ctx, f.RecordID, f.VersionID)
			return err
		})
		if !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("disposed read succeeded", err)
		}
	}
	testDisposedProjectionReads(t, ctx, admin, cipher, c.Tenant, accepted.LedgerRevision, memoryAccepted.LedgerRevision, fv, mv)
	replay()
	// A fresh archive payload contains metadata and an explicit coverage gap.
	for _, revision := range []int64{f.LedgerRevision, accepted.LedgerRevision, m.LedgerRevision} {
		var event model.Event
		var b []byte
		if err := admin.QueryRow(ctx, "SELECT payload FROM ledger_events WHERE tenant_id=$1 AND ledger_revision=$2", c.Tenant, revision).Scan(&b); err != nil {
			t.Fatal(err)
		}
		plain, err := cipher.Open(c.Tenant, b)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(plain, &event); err != nil {
			t.Fatal(err)
		}
		payload, err := ledger.ArchivePayload(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant}, Event: event})
		if err != nil {
			t.Fatal(err)
		}
		gap := "version_content"
		if revision == m.LedgerRevision {
			gap = "memory_version_content"
		}
		found := false
		for _, g := range payload.Gaps {
			if g.Kind == gap && g.Reason == "disposed_under_retention_policy" {
				found = true
			}
		}
		if !found {
			t.Fatal("archive omitted disposal gap")
		}
	}
	// Full signed export exercises the service's disposal-gap filtering.
	engine, err := facts.NewWithAudit(ledger, allow{}, store, model.AuditOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	job, err := engine.StartAuditExport(ctx, c, model.AuditExportRequest{Command: command("export"), Scope: "scope", IncludeEvidence: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.RunAuditExport(ctx, c, model.AuditExportQuery{ID: job.ID, Scope: "scope", Purpose: "retention-test"})
	if err != nil {
		t.Fatal(err)
	}
	gaps := map[string]bool{}
	for _, g := range result.Bundle.Manifest.Gaps {
		gaps[g.Kind] = true
	}
	if !gaps["version_content"] || !gaps["memory_version_content"] {
		t.Fatal("export missing disposal coverage", gaps)
	}
}
