package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedCalculationPayloadMigration(t *testing.T, ctx context.Context, admin *pgxpool.Pool) map[string][]byte {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	out := map[string][]byte{}
	for _, id := range []string{"legacy-calculation", "opaque-calculation"} {
		r := model.CalculationRecord{ID: id, Tenant: "legacy", LedgerRevision: 2, Receipt: model.CalculationReceipt{Scope: "scope", ApplicationID: "application", ExternalRunID: id, SnapshotID: "legacy-snapshot-a"}}
		r.Digest = model.CalculationDigest(r.Receipt)
		plain, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := cipher.Seal("legacy", plain)
		if err != nil {
			t.Fatal(err)
		}
		if id == "opaque-calculation" {
			payload = []byte{1, 2, 3}
		}
		if _, err = admin.Exec(ctx, "INSERT INTO calculation_receipts(tenant_id,id,scope_id,snapshot_id,application_id,external_run_id,request_digest,ledger_revision,payload) VALUES('legacy',$1,'scope','legacy-snapshot-a','application',$1,$2,2,$3)", id, r.Digest, payload); err != nil {
			t.Fatal(err)
		}
		out[id] = payload
	}
	return out
}

func testCalculationPayloadMigration(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool, want map[string][]byte) {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	for id, expected := range want {
		var payload []byte
		if err = admin.QueryRow(ctx, "SELECT payload FROM calculation_payloads WHERE tenant_id='legacy' AND calculation_id=$1", id).Scan(&payload); err != nil || !bytes.Equal(payload, expected) {
			t.Fatal("migration changed ciphertext", id, err)
		}
	}
	var count int
	if err = admin.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='calculation_receipts' AND column_name='payload'").Scan(&count); err != nil || count != 0 {
		t.Fatal("calculation header retains content", count, err)
	}
	read := func(byRun bool) error {
		return ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
			var r model.CalculationRecord
			var err error
			if byRun {
				r, err = tx.(model.AuditTx).CalculationRun(ctx, "application", "legacy-calculation")
			} else {
				r, err = tx.(model.AuditTx).Calculation(ctx, "legacy-calculation")
			}
			if err == nil && (r.ID != "legacy-calculation" || r.Tenant != "legacy" || model.CalculationDigest(r.Receipt) != r.Digest) {
				t.Fatal("calculation altered", r)
			}
			return err
		})
	}
	for _, byRun := range []bool{false, true} {
		if err = read(byRun); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = ledgerdb.New(tx).BindTenant(ctx, "other"); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM calculation_payloads WHERE tenant_id='legacy'").Scan(&count); err != nil || count != 0 {
		t.Fatal("calculation tenant leak", count, err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"DELETE FROM calculation_payloads", "UPDATE calculation_payloads SET payload=decode('00','hex')", "TRUNCATE calculation_payloads"} {
		_, err = runtime.Exec(ctx, query)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
			t.Fatal("runtime can mutate calculation ciphertext", query, err)
		}
	}
	if _, err = admin.Exec(ctx, "DELETE FROM calculation_payloads WHERE tenant_id='legacy' AND calculation_id='legacy-calculation'"); err != nil {
		t.Fatal(err)
	}
	for _, byRun := range []bool{false, true} {
		if err = read(byRun); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("missing calculation bytes not explicit", byRun, err)
		}
	}
	// Both inventories retain a structural row even when its ciphertext is missing.
	for _, query := range []struct {
		sql string
		arg any
	}{{ledgerdb.ListAuditCalculation, "scope"}, {ledgerdb.ListArchiveCalculation, int64(2)}} {
		rows, err := admin.Query(ctx, query.sql, "legacy", query.arg)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for rows.Next() {
			var id string
			var payload []byte
			if err = rows.Scan(&id, &payload); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if id == "legacy-calculation" {
				found = true
				if payload != nil {
					rows.Close()
					t.Fatal("unexpected missing payload")
				}
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil || !found {
			t.Fatal("inventory hid missing calculation", err)
		}
	}
	if _, err = admin.Exec(ctx, "INSERT INTO calculation_payloads(tenant_id,calculation_id,payload) VALUES('legacy','legacy-calculation',$1)", want["legacy-calculation"]); err != nil {
		t.Fatal(err)
	}
	if err = read(false); err != nil {
		t.Fatal(err)
	}
}

func testCalculationPayloadAtomicity(t *testing.T, ctx context.Context, engine *facts.Engine, admin *pgxpool.Pool, c model.Caller, r model.CalculationReceipt) {
	t.Helper()
	r.Key = "missing-calculation-payload-grant"
	r.ExternalRunID = "missing-calculation-payload-grant"
	var before, after int64
	if err := admin.QueryRow(ctx, "SELECT revision FROM tenant_revisions WHERE tenant_id=$1", c.Tenant).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "REVOKE INSERT ON calculation_payloads FROM facts_runtime"); err != nil {
		t.Fatal(err)
	}
	_, recordErr := engine.RecordCalculation(ctx, c, r)
	if _, err := admin.Exec(ctx, "GRANT INSERT ON calculation_payloads TO facts_runtime"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(recordErr, model.ErrDenied) {
		t.Fatal("payload insert did not fail", recordErr)
	}
	if err := admin.QueryRow(ctx, "SELECT revision FROM tenant_revisions WHERE tenant_id=$1", c.Tenant).Scan(&after); err != nil || before != after {
		t.Fatal("failed payload advanced ledger", before, after, err)
	}
	var count int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM calculation_receipts WHERE tenant_id=$1 AND application_id=$2 AND external_run_id=$3", c.Tenant, r.ApplicationID, r.ExternalRunID).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed payload retained header", count, err)
	}
}
