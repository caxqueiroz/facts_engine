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

func seedDecisionPayloadMigration(t *testing.T, ctx context.Context, admin *pgxpool.Pool) map[string][]byte {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	out := map[string][]byte{}
	for _, id := range []string{"legacy-decision", "opaque-decision"} {
		r := model.DecisionRecord{ID: id, Tenant: "legacy", LedgerRevision: 2, Receipt: model.DecisionReceipt{Scope: "scope", ApplicationID: "application", ExternalDecisionID: id, ContextID: "legacy-context-a"}}
		r.Digest = model.DecisionDigest(r.Receipt)
		plain, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := cipher.Seal("legacy", plain)
		if err != nil {
			t.Fatal(err)
		}
		if id == "opaque-decision" {
			payload = []byte{1, 2, 3}
		}
		if _, err = admin.Exec(ctx, "INSERT INTO decision_receipts(tenant_id,id,scope_id,context_id,application_id,external_decision_id,request_digest,ledger_revision,payload) VALUES('legacy',$1,'scope','legacy-context-a','application',$1,$2,2,$3)", id, r.Digest, payload); err != nil {
			t.Fatal(err)
		}
		if _, err = admin.Exec(ctx, "INSERT INTO decision_scopes(tenant_id,decision_id,scope_id) VALUES('legacy',$1,'scope')", id); err != nil {
			t.Fatal(err)
		}
		out[id] = payload
	}
	return out
}

func testDecisionPayloadMigration(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool, want map[string][]byte) {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	for id, expected := range want {
		var payload []byte
		if err = admin.QueryRow(ctx, "SELECT payload FROM decision_payloads WHERE tenant_id='legacy' AND decision_id=$1", id).Scan(&payload); err != nil || !bytes.Equal(payload, expected) {
			t.Fatal("migration changed ciphertext", id, err)
		}
	}
	var count int
	if err = admin.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='decision_receipts' AND column_name='payload'").Scan(&count); err != nil || count != 0 {
		t.Fatal("decision header retains content", count, err)
	}
	read := func(byRun bool) error {
		return ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
			var r model.DecisionRecord
			var err error
			if byRun {
				r, err = tx.(model.ContextTx).DecisionRun(ctx, "application", "legacy-decision")
			} else {
				r, err = tx.(model.ContextTx).Decision(ctx, "legacy-decision")
			}
			if err == nil && (r.ID != "legacy-decision" || r.Tenant != "legacy" || model.DecisionDigest(r.Receipt) != r.Digest) {
				t.Fatal("decision altered", r)
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
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM decision_payloads WHERE tenant_id='legacy'").Scan(&count); err != nil || count != 0 {
		t.Fatal("decision tenant leak", count, err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"DELETE FROM decision_payloads", "UPDATE decision_payloads SET payload=decode('00','hex')", "TRUNCATE decision_payloads"} {
		_, err = runtime.Exec(ctx, query)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
			t.Fatal("runtime can mutate decision ciphertext", query, err)
		}
	}
	if _, err = admin.Exec(ctx, "DELETE FROM decision_payloads WHERE tenant_id='legacy' AND decision_id='legacy-decision'"); err != nil {
		t.Fatal(err)
	}
	for _, byRun := range []bool{false, true} {
		if err = read(byRun); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("missing decision bytes not explicit", byRun, err)
		}
	}
	// Both inventories retain a structural row even when its ciphertext is missing.
	for _, query := range []struct {
		sql string
		arg any
	}{{ledgerdb.ListAuditDecision, "scope"}, {ledgerdb.ListArchiveDecision, int64(2)}} {
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
			if id == "legacy-decision" {
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
			t.Fatal("inventory hid missing decision", err)
		}
	}
	if _, err = admin.Exec(ctx, "INSERT INTO decision_payloads(tenant_id,decision_id,payload) VALUES('legacy','legacy-decision',$1)", want["legacy-decision"]); err != nil {
		t.Fatal(err)
	}
	if err = read(false); err != nil {
		t.Fatal(err)
	}
}

func testDecisionPayloadAtomicity(t *testing.T, ctx context.Context, engine *facts.Engine, admin *pgxpool.Pool, c model.Caller, r model.DecisionReceipt) {
	t.Helper()
	r.Key = "missing-decision-payload-grant"
	r.ExternalDecisionID = "missing-decision-payload-grant"
	var before, after int64
	if err := admin.QueryRow(ctx, "SELECT revision FROM tenant_revisions WHERE tenant_id=$1", c.Tenant).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "REVOKE INSERT ON decision_payloads FROM facts_runtime"); err != nil {
		t.Fatal(err)
	}
	_, recordErr := engine.RecordDecision(ctx, c, r)
	if _, err := admin.Exec(ctx, "GRANT INSERT ON decision_payloads TO facts_runtime"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(recordErr, model.ErrDenied) {
		t.Fatal("payload insert did not fail", recordErr)
	}
	if err := admin.QueryRow(ctx, "SELECT revision FROM tenant_revisions WHERE tenant_id=$1", c.Tenant).Scan(&after); err != nil || before != after {
		t.Fatal("failed payload advanced ledger", before, after, err)
	}
	var count int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM decision_receipts WHERE tenant_id=$1 AND application_id=$2 AND external_decision_id=$3", c.Tenant, r.ApplicationID, r.ExternalDecisionID).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed payload retained header", count, err)
	}
}
