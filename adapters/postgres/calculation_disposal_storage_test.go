package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testCalculationDisposalStorage(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool) {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	var original model.CalculationRecord
	if err = ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
		var err error
		original, err = tx.(model.AuditTx).Calculation(ctx, "legacy-calculation")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	tombstone := model.CalculationTombstone(original)
	raw, err := json.Marshal(tombstone)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := cipher.Seal("legacy", raw)
	if err != nil {
		t.Fatal(err)
	}
	decisionSQL := "INSERT INTO decision_receipts(tenant_id,id,scope_id,application_id,external_decision_id,request_digest,snapshot_id,calculation_id,ledger_revision) VALUES('legacy','guard-decision','scope','app','guard-decision','digest','legacy-snapshot-a','legacy-calculation',2)"
	// A live calculation accepts references; roll back this fixture before disposal.
	live, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Rollback(ctx)
	if _, err = live.Exec(ctx, decisionSQL); err != nil {
		t.Fatal(err)
	}
	if err = live.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	stale, err := admin.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Rollback(ctx)
	var visible int
	if err = stale.QueryRow(ctx, "SELECT count(*) FROM calculation_payloads WHERE tenant_id='legacy' AND calculation_id='legacy-calculation'").Scan(&visible); err != nil || visible != 1 {
		t.Fatal(visible, err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO calculation_disposals(tenant_id,calculation_id,payload) VALUES('legacy','legacy-calculation',$1)", payload); err != nil {
		t.Fatal(err)
	}
	// An invalid state retaining both copies must never disclose the live payload.
	for _, byRun := range []bool{false, true} {
		err = ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
			if byRun {
				_, err := tx.(model.AuditTx).CalculationRun(ctx, "application", "legacy-calculation")
				return err
			}
			_, err := tx.(model.AuditTx).Calculation(ctx, "legacy-calculation")
			return err
		})
		if !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("marked content disclosed", byRun, err)
		}
	}
	_, err = stale.Exec(ctx, decisionSQL)
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "40001" {
		t.Fatal("stale reference crossed disposal", err)
	}
	if err = stale.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{decisionSQL, "UPDATE calculation_payloads SET payload=payload WHERE tenant_id='legacy' AND calculation_id='legacy-calculation'", "INSERT INTO calculation_payloads SELECT * FROM calculation_payloads WHERE tenant_id='legacy' AND calculation_id='legacy-calculation' ON CONFLICT DO NOTHING"} {
		_, err = admin.Exec(ctx, query)
		if !errors.As(err, &pgerr) || pgerr.Code != "23514" {
			t.Fatal("disposed content accepted reference or reinsert", err)
		}
	}
	// Inventory rows prefer the structural tombstone even while old bytes remain.
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
			var content []byte
			if err = rows.Scan(&id, &content); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if id == original.ID {
				found = true
				if !bytes.Equal(content, payload) {
					rows.Close()
					t.Fatal("inventory exposed live payload")
				}
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil || !found {
			t.Fatal("missing tombstone inventory", err)
		}
	}
	for _, query := range []string{"DELETE FROM calculation_disposals", "UPDATE calculation_disposals SET payload=decode('01','hex')", "DELETE FROM calculation_guards", "TRUNCATE calculation_guards"} {
		_, err = runtime.Exec(ctx, query)
		if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
			t.Fatal("runtime can remove disposal fence", err)
		}
	}
	// Only the fixture owner restores this synthetic marker; runtime has no such permission.
	if _, err = admin.Exec(ctx, "DELETE FROM calculation_disposals WHERE tenant_id='legacy' AND calculation_id='legacy-calculation'"); err != nil {
		t.Fatal(err)
	}
}
