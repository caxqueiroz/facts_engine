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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testExtractionInputInventories(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool) {
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	rows, err := admin.Query(ctx, `SELECT a.tenant_id,a.id,p.payload,i.request_digest,i.fact_count,i.memory_count,
 (SELECT count(*) FROM extraction_attempt_fact_inputs f WHERE f.tenant_id=a.tenant_id AND f.attempt_id=a.id),
 (SELECT count(*) FROM extraction_attempt_memory_inputs m WHERE m.tenant_id=a.tenant_id AND m.attempt_id=a.id)
 FROM extraction_attempts a LEFT JOIN extraction_attempt_payloads p ON p.tenant_id=a.tenant_id AND p.attempt_id=a.id LEFT JOIN extraction_attempt_inventories i ON i.tenant_id=a.tenant_id AND i.attempt_id=a.id`)
	if err != nil {
		t.Fatal(err)
	}
	total, facts, memories := 0, 0, 0
	firstTenant, firstID, firstDigest := "", "", ""
	var firstFacts, firstMemories int
	for rows.Next() {
		var tenant, id, digest string
		var payload []byte
		var fc, mc, actualFacts, actualMemories int
		if err := rows.Scan(&tenant, &id, &payload, &digest, &fc, &mc, &actualFacts, &actualMemories); err != nil {
			rows.Close()
			t.Fatal("attempt lacks inventory", err)
		}
		plain, err := cipher.Open(tenant, payload)
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		var a model.ExtractionAttempt
		if err := json.Unmarshal(plain, &a); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if digest != model.BytesDigest(a.RequestBytes) || digest != a.RequestDigest || fc != actualFacts || mc != actualMemories {
			rows.Close()
			t.Fatal("inventory not bound to retained input")
		}
		if total == 0 {
			firstTenant, firstID, firstDigest, firstFacts, firstMemories = tenant, id, digest, fc, mc
		}
		total++
		facts += fc
		memories += mc
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	if total == 0 || facts == 0 || memories == 0 {
		t.Fatal("fixtures did not exercise inventory classes", total, facts, memories)
	}
	for _, table := range []string{"extraction_attempt_fact_inputs", "extraction_attempt_memory_inputs"} {
		var omitted int
		if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE NOT payload_retained").Scan(&omitted); err != nil || omitted == 0 {
			t.Fatal("approved omission not inventoried", table, omitted, err)
		}
	}
	for _, kind := range []string{"fact", "memory"} {
		table := "extraction_attempt_" + kind + "_inputs"
		var tenant, attempt, version string
		if err := admin.QueryRow(ctx, "SELECT tenant_id,attempt_id,version_id FROM "+table+" WHERE NOT payload_retained LIMIT 1").Scan(&tenant, &attempt, &version); err != nil {
			t.Fatal(err)
		}
		tx, err := admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		if _, err := tx.Exec(ctx, "INSERT INTO "+kind+"_version_disposals(tenant_id,version_id,payload) VALUES($1,$2,$3)", tenant, version, []byte{1}); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, "UPDATE "+table+" SET payload_retained=true WHERE tenant_id=$1 AND attempt_id=$2 AND version_id=$3", tenant, attempt, version)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "23514" {
			t.Fatal("disposed input was promoted to retained content", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"extraction_attempt_inventories", "extraction_attempt_fact_inputs", "extraction_attempt_memory_inputs"} {
		for _, query := range []string{"DELETE FROM " + table, "TRUNCATE " + table} {
			_, err := runtime.Exec(ctx, query)
			var pg *pgconn.PgError
			if !errors.As(err, &pg) || pg.Code != "42501" {
				t.Fatal("runtime rewrote inventory", err)
			}
		}
		tx, err := runtime.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := ledgerdb.New(tx).BindTenant(ctx, "inventory-other"); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatal("cross-tenant inventory leak", count, err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// A pre-migration attempt must remain an explicit inventory gap, even when
	// its current row counts happen to be zero. Never infer completeness.
	if _, err := admin.Exec(ctx, "DELETE FROM extraction_attempt_inventories WHERE tenant_id=$1 AND attempt_id=$2", firstTenant, firstID); err != nil {
		t.Fatal(err)
	}
	gap, err := ledgerdb.New(admin).HasUninventoriedExtractionAttempts(ctx, firstTenant)
	if err != nil || !gap {
		t.Fatal("legacy inventory gap hidden", gap, err)
	}
	if err := ledgerdb.New(admin).InsertExtractionInputInventory(ctx, ledgerdb.InsertExtractionInputInventoryParams{TenantID: firstTenant, AttemptID: firstID, RequestDigest: firstDigest, FactCount: int32(firstFacts), MemoryCount: int32(firstMemories)}); err != nil {
		t.Fatal(err)
	}
	gap, err = ledgerdb.New(admin).HasUninventoriedExtractionAttempts(ctx, firstTenant)
	if err != nil || gap {
		t.Fatal("complete inventory not recognized", gap, err)
	}
}
