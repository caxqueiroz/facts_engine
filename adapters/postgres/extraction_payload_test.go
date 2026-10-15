package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedExtractionPayloadMigration(t *testing.T, ctx context.Context, admin *pgxpool.Pool) map[string][]byte {
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	seal := func(value any) []byte {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		b, err := cipher.Seal("legacy", raw)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	caller := model.Caller{Tenant: "legacy", Principal: "worker"}
	profile := model.ExtractionProfile{ID: "legacy-profile"}
	job := model.ExtractionJob{ID: "payload-job", Tenant: "legacy", Caller: caller, Profile: profile, ProfileDigest: model.ExtractionProfileDigest(profile), CreatedRevision: 3, Request: model.ExtractionRequest{Command: model.Command{Key: "payload-job", Purpose: "migration"}, Scope: "scope", ConversationID: "payload-conversation", ProfileID: profile.ID}}
	input := model.ExtractionInput{Schema: "facts-extraction-input-v1", Tenant: "legacy", JobID: job.ID, AttemptID: "payload-attempt", Profile: profile, Messages: []model.ExtractionInputMessage{{Content: "legacy private request"}}}
	request, _ := json.Marshal(input)
	attempt := model.ExtractionAttempt{ID: "payload-attempt", Tenant: "legacy", Scope: "scope", JobID: job.ID, Caller: caller, Fence: 1, LedgerRevision: 3, RequestBytes: request, RequestDigest: model.BytesDigest(request)}
	response := []byte(`{"private":"legacy response"}`)
	result := model.ExtractionResult{AttemptID: attempt.ID, JobID: job.ID, Tenant: "legacy", Scope: "scope", Outcome: "completed", ResponseComplete: true, RequestDigest: attempt.RequestDigest, ResponseBytes: response, ResponseDigest: model.BytesDigest(response), LedgerRevision: 3}
	values := map[string][]byte{"job": seal(job), "attempt": seal(attempt), "result": seal(result)}
	conversation := model.ConversationRecord{ID: "payload-conversation", Tenant: "legacy", Caller: caller, Request: model.ConversationRequest{Scope: "scope"}}
	if _, err := admin.Exec(ctx, "INSERT INTO conversations(tenant_id,id,scope_id,ledger_revision,payload) VALUES('legacy','payload-conversation','scope',3,$1)", seal(conversation)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO extraction_jobs(tenant_id,id,scope_id,conversation_id,profile_id,profile_digest,ledger_revision,payload) VALUES('legacy','payload-job','scope','payload-conversation',$1,$2,3,$3)", profile.ID, job.ProfileDigest, values["job"]); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO extraction_attempts(tenant_id,id,job_id,scope_id,fence,ledger_revision,payload) VALUES('legacy','payload-attempt','payload-job','scope',1,3,$1)", values["attempt"]); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO extraction_results(tenant_id,attempt_id,job_id,scope_id,ledger_revision,payload) VALUES('legacy','payload-attempt','payload-job','scope',3,$1)", values["result"]); err != nil {
		t.Fatal(err)
	}
	return values
}

func testExtractionPayloadMigration(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool, want map[string][]byte) {
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	read := func(kind string) ([]byte, error) {
		var raw []byte
		err := ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
			var value any
			var err error
			switch kind {
			case "job":
				value, err = tx.(model.ExtractionTx).ExtractionJob(ctx, "payload-job")
			case "attempt":
				value, err = tx.(model.ExtractionRunTx).ExtractionAttempt(ctx, "payload-attempt")
			case "result":
				value, err = tx.(model.ExtractionRunTx).ExtractionResult(ctx, "payload-attempt")
			}
			if err != nil {
				return err
			}
			raw, err = json.Marshal(value)
			return err
		})
		return raw, err
	}
	for _, f := range []struct{ kind, parent, key, id, archiveQuery, auditQuery string }{
		{"job", "extraction_jobs", "job_id", "payload-job", ledgerdb.ListArchiveExtractionJob, ledgerdb.ListAuditExtractionJob},
		{"attempt", "extraction_attempts", "attempt_id", "payload-attempt", ledgerdb.ListArchiveExtractionAttempt, ledgerdb.ListAuditExtractionAttempt},
		{"result", "extraction_results", "attempt_id", "payload-attempt", ledgerdb.ListArchiveExtractionResult, ledgerdb.ListAuditExtractionResult},
	} {
		t.Run(f.kind, func(t *testing.T) {
			table := "extraction_" + f.kind + "_payloads"
			original, err := cipher.Open("legacy", want[f.kind])
			if err != nil {
				t.Fatal(err)
			}
			raw, err := read(f.kind)
			if err != nil || !bytes.Equal(raw, original) {
				t.Fatal("decoded historical artifact changed", err)
			}
			var encrypted []byte
			if err := admin.QueryRow(ctx, fmt.Sprintf("SELECT payload FROM %s WHERE tenant_id='legacy' AND %s=$1", table, f.key), f.id).Scan(&encrypted); err != nil || !bytes.Equal(encrypted, want[f.kind]) {
				t.Fatal("migration changed ciphertext", err)
			}
			var count int
			if err := admin.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND column_name='payload'", f.parent).Scan(&count); err != nil || count != 0 {
				t.Fatal("header retains payload", err)
			}
			for _, query := range []string{"DELETE FROM " + table, "UPDATE " + table + " SET payload=decode('00','hex')", "TRUNCATE " + table} {
				_, err := runtime.Exec(ctx, query)
				var pg *pgconn.PgError
				if !errors.As(err, &pg) || pg.Code != "42501" {
					t.Fatal("runtime changed immutable payload", err)
				}
			}
			tx, err := runtime.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := ledgerdb.New(tx).BindTenant(ctx, "other-extraction-payload-tenant"); err != nil {
				t.Fatal(err)
			}
			if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
				t.Fatal("tenant leak", count, err)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			// A disposal marker is not a usable model artifact, even if the
			// envelope still contains its original bytes.
			var marked map[string]any
			if err := json.Unmarshal(original, &marked); err != nil {
				t.Fatal(err)
			}
			marked["PayloadDisposed"] = true
			markedRaw, err := json.Marshal(marked)
			if err != nil {
				t.Fatal(err)
			}
			markedBytes, err := cipher.Seal("legacy", markedRaw)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := admin.Exec(ctx, fmt.Sprintf("UPDATE %s SET payload=$2 WHERE tenant_id='legacy' AND %s=$1", table, f.key), f.id, markedBytes); err != nil {
				t.Fatal(err)
			}
			if _, err := read(f.kind); !errors.Is(err, model.ErrPrecondition) {
				t.Fatal("disposed artifact exposed", err)
			}
			if f.kind == "attempt" {
				if _, err := ledger.BackfillExtractionInputInventories(ctx, "legacy", 1); !errors.Is(err, model.ErrInvalid) {
					t.Fatal("disposed request backfilled", err)
				}
			}
			if _, err := admin.Exec(ctx, fmt.Sprintf("UPDATE %s SET payload=$2 WHERE tenant_id='legacy' AND %s=$1", table, f.key), f.id, want[f.kind]); err != nil {
				t.Fatal(err)
			}
			if _, err := admin.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE tenant_id='legacy' AND %s=$1", table, f.key), f.id); err != nil {
				t.Fatal(err)
			}
			if _, err := read(f.kind); !errors.Is(err, model.ErrPrecondition) {
				t.Fatal("missing artifact not reported", err)
			}
			if f.kind == "attempt" {
				if _, err := ledger.BackfillExtractionInputInventories(ctx, "legacy", 1); !errors.Is(err, model.ErrPrecondition) {
					t.Fatal("missing request treated as inventoried", err)
				}
			}
			for i, query := range []string{f.archiveQuery, f.auditQuery} {
				var arg any = int64(3)
				if i == 1 {
					arg = "scope"
				}
				rows, err := admin.Query(ctx, query, "legacy", arg)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for rows.Next() {
					var id string
					var payload []byte
					if err := rows.Scan(&id, &payload); err != nil {
						rows.Close()
						t.Fatal(err)
					}
					if id == f.id {
						found = true
						if payload != nil {
							rows.Close()
							t.Fatal("unexpected payload")
						}
					}
				}
				err = rows.Err()
				rows.Close()
				if err != nil || !found {
					t.Fatal("missing artifact vanished from audit/archive", err)
				}
			}
			if _, err := admin.Exec(ctx, fmt.Sprintf("INSERT INTO %s(tenant_id,%s,payload) VALUES('legacy',$1,$2)", table, f.key), f.id, want[f.kind]); err != nil {
				t.Fatal(err)
			}
		})
	}
	page, err := ledger.BackfillExtractionInputInventories(ctx, "legacy", 1)
	if err != nil || page.Inventoried != 1 || !page.Complete {
		t.Fatal("migrated request backfill", page, err)
	}
}
