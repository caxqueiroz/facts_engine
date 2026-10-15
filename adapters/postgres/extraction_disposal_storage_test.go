package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"

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

func testExtractionDisposalStorage(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool, store *evidence.Postgres) {
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	var tenant, jobID, attemptID string
	if err := admin.QueryRow(ctx, `SELECT j.tenant_id,j.id,a.id FROM extraction_jobs j
 JOIN extraction_job_state s ON s.tenant_id=j.tenant_id AND s.id=j.id
 JOIN extraction_attempts a ON a.tenant_id=j.tenant_id AND a.job_id=j.id
 JOIN extraction_results r ON r.tenant_id=a.tenant_id AND r.attempt_id=a.id
 WHERE j.tenant_id='bank1' AND s.state='completed' AND j.conversation_id IS NOT NULL
 AND NOT EXISTS(SELECT 1 FROM extraction_memory_targets t WHERE t.tenant_id=j.tenant_id AND t.job_id=j.id)
 AND (SELECT count(*) FROM extraction_attempts x WHERE x.tenant_id=j.tenant_id AND x.job_id=j.id)=1
 ORDER BY j.id LIMIT 1`).Scan(&tenant, &jobID, &attemptID); err != nil {
		t.Fatal(err)
	}
	var job model.ExtractionJob
	var attempt model.ExtractionAttempt
	var result model.ExtractionResult
	if err := ledger.Transact(ctx, tenant, func(tx model.Tx) error {
		run := tx.(model.ExtractionRunTx)
		var err error
		job, err = run.ExtractionJob(ctx, jobID)
		if err != nil {
			return err
		}
		attempt, err = run.ExtractionAttempt(ctx, attemptID)
		if err != nil {
			return err
		}
		result, err = run.ExtractionResult(ctx, attemptID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := audit.NewEd25519("extraction-key", private)
	replay := archiveRevisionsBeforeDisposal(t, ctx, admin, cipher, signer, map[string]ed25519.PublicKey{"extraction-key": public}, tenant, job.CreatedRevision, attempt.LedgerRevision, result.LedgerRevision)
	stale, err := runtime.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Rollback(ctx)
	if err := ledgerdb.New(stale).BindTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := stale.QueryRow(ctx, "SELECT count(*) FROM extraction_job_payloads WHERE tenant_id=$1 AND job_id=$2", tenant, jobID).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	type target struct {
		kind, key, id string
		metadata      any
	}
	targets := []target{{"result", "attempt_id", attemptID, model.ExtractionResultTombstone(result)}, {"attempt", "attempt_id", attemptID, model.ExtractionAttemptTombstone(attempt)}, {"job", "job_id", jobID, model.ExtractionJobTombstone(job)}}
	retained := map[string][]byte{}
	// Owner SQL simulates storage only; no policy/hold authorization is implied.
	for pass := 0; pass < 2; pass++ {
		tx, err := admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, target := range targets {
			table := "extraction_" + target.kind
			raw, _ := json.Marshal(target.metadata)
			sealed, err := cipher.Seal(tenant, raw)
			if err != nil {
				t.Fatal(err)
			}
			if pass == 0 {
				var b []byte
				if err := tx.QueryRow(ctx, "SELECT payload FROM "+table+"_payloads WHERE tenant_id=$1 AND "+target.key+"=$2", tenant, target.id).Scan(&b); err != nil {
					t.Fatal(err)
				}
				retained[target.kind] = b
			}
			if _, err := tx.Exec(ctx, "INSERT INTO "+table+"_disposals(tenant_id,"+target.key+",payload) VALUES($1,$2,$3)", tenant, target.id, sealed); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, "DELETE FROM "+table+"_payloads WHERE tenant_id=$1 AND "+target.key+"=$2", tenant, target.id); err != nil {
				t.Fatal(err)
			}
		}
		if pass == 0 {
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if err := admin.QueryRow(ctx, "SELECT count(*) FROM extraction_job_disposals WHERE tenant_id=$1 AND job_id=$2", tenant, jobID).Scan(&count); err != nil || count != 0 {
				t.Fatal("rollback left tombstone", err)
			}
		} else if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	lateAttempt := `INSERT INTO extraction_attempts(tenant_id,id,job_id) VALUES($1,'late-retention-attempt',$2)`
	_, err = stale.Exec(ctx, lateAttempt, tenant, jobID)
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "40001" {
		t.Fatal("stale provider admission bypassed disposal", err)
	}
	if err := stale.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{lateAttempt, `INSERT INTO extraction_results(tenant_id,job_id,attempt_id) VALUES($1,$2,'late-retention-attempt')`, `UPDATE extraction_job_state SET state='pending' WHERE tenant_id=$1 AND id=$2`} {
		tx, err := runtime.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := ledgerdb.New(tx).BindTenant(ctx, tenant); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, query, tenant, jobID)
		if !errors.As(err, &pg) || pg.Code != "23514" {
			t.Fatal("late execution was not fenced", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, target := range targets {
		table := "extraction_" + target.kind
		_, err := admin.Exec(ctx, "INSERT INTO "+table+"_payloads(tenant_id,"+target.key+",payload) VALUES($1,$2,$3)", tenant, target.id, retained[target.kind])
		if !errors.As(err, &pg) || pg.Code != "23514" {
			t.Fatal("payload resurrected", target.kind, err)
		}
		for _, query := range []string{"INSERT INTO " + table + "_disposals(tenant_id," + target.key + ",payload) VALUES('other','other',decode('00','hex'))", "UPDATE " + table + "_disposals SET payload=decode('00','hex')", "DELETE FROM " + table + "_disposals", "TRUNCATE " + table + "_disposals"} {
			_, err := runtime.Exec(ctx, query)
			if !errors.As(err, &pg) || pg.Code != "42501" {
				t.Fatal("runtime changed disposal metadata", err)
			}
		}
		other, err := runtime.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := ledgerdb.New(other).BindTenant(ctx, "other-extraction-disposal"); err != nil {
			t.Fatal(err)
		}
		if err := other.QueryRow(ctx, "SELECT count(*) FROM "+table+"_disposals").Scan(&count); err != nil || count != 0 {
			t.Fatal("disposal metadata tenant leak", count, err)
		}
		if err := other.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		err = ledger.Transact(ctx, tenant, func(tx model.Tx) error {
			run := tx.(model.ExtractionRunTx)
			switch target.kind {
			case "job":
				_, err := run.ExtractionJob(ctx, target.id)
				return err
			case "attempt":
				_, err := run.ExtractionAttempt(ctx, target.id)
				return err
			default:
				_, err := run.ExtractionResult(ctx, target.id)
				return err
			}
		})
		if !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("disposed artifact read", target.kind, err)
		}
	}
	replay()
	for _, revision := range []int64{job.CreatedRevision, attempt.LedgerRevision, result.LedgerRevision} {
		var payload []byte
		if err := admin.QueryRow(ctx, "SELECT payload FROM ledger_events WHERE tenant_id=$1 AND ledger_revision=$2", tenant, revision).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		plain, err := cipher.Open(tenant, payload)
		if err != nil {
			t.Fatal(err)
		}
		var event model.Event
		if err := json.Unmarshal(plain, &event); err != nil {
			t.Fatal(err)
		}
		archived, err := ledger.ArchivePayload(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: tenant}, Event: event})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, gap := range archived.Gaps {
			if gap.Reason == "disposed_under_retention_policy" && (gap.ID == jobID || gap.ID == attemptID) {
				found = true
			}
		}
		if !found {
			t.Fatal("archive omitted content gap", revision)
		}
	}
	engine, err := facts.NewWithAudit(ledger, allow{}, store, model.AuditOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	caller := model.Caller{Tenant: tenant, Principal: "auditor"}
	scheduled, err := engine.StartAuditExport(ctx, caller, model.AuditExportRequest{Command: model.Command{Key: "extraction-storage-export", Purpose: "audit"}, Scope: job.Request.Scope, IncludeEvidence: true})
	if err != nil {
		t.Fatal(err)
	}
	exported, err := engine.RunAuditExport(ctx, caller, model.AuditExportQuery{ID: scheduled.ID, Scope: job.Request.Scope, Purpose: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	gaps := map[string]bool{}
	for _, gap := range exported.Bundle.Manifest.Gaps {
		if gap.ID == jobID || gap.ID == attemptID {
			gaps[gap.Kind] = true
		}
	}
	for _, kind := range []string{"extraction_job_content", "extraction_attempt_content", "extraction_result_content"} {
		if !gaps[kind] {
			t.Fatal("export missing disposal gap", kind, gaps)
		}
	}
}
