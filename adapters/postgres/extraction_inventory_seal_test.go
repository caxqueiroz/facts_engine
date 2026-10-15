package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testExtractionInventorySealing(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool) {
	for _, kind := range []string{"fact", "memory"} {
		t.Run(kind, func(t *testing.T) {
			table := "extraction_attempt_" + kind + "_inputs"
			var tenant, attempt, record, version, digest string
			var retained bool
			var facts, memories int32
			if err := admin.QueryRow(ctx, "SELECT f.tenant_id,f.attempt_id,f.record_id,f.version_id,f.payload_retained,i.request_digest,i.fact_count,i.memory_count FROM "+table+" f JOIN extraction_attempt_inventories i USING(tenant_id,attempt_id) LIMIT 1").Scan(&tenant, &attempt, &record, &version, &retained, &digest, &facts, &memories); err != nil {
				t.Fatal(err)
			}
			insert := func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, "INSERT INTO "+table+"(tenant_id,attempt_id,record_id,version_id,payload_retained) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING", tenant, attempt, record, version, retained)
				return err
			}
			tx, err := runtime.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := ledgerdb.New(tx).BindTenant(ctx, tenant); err != nil {
				t.Fatal(err)
			}
			var pg *pgconn.PgError
			if err := insert(tx); !errors.As(err, &pg) || pg.Code != "23514" {
				t.Fatal("completed inventory allowed another insertion", err)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			// Simulate an incomplete legacy inventory without touching its reference rows.
			if _, err := admin.Exec(ctx, "DELETE FROM extraction_attempt_inventories WHERE tenant_id=$1 AND attempt_id=$2", tenant, attempt); err != nil {
				t.Fatal(err)
			}
			tx, err = runtime.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := ledgerdb.New(tx).BindTenant(ctx, tenant); err != nil {
				t.Fatal(err)
			}
			params := ledgerdb.InsertExtractionInputInventoryParams{TenantID: tenant, AttemptID: attempt, RequestDigest: digest, FactCount: facts + 1, MemoryCount: memories}
			if err := ledgerdb.New(tx).InsertExtractionInputInventory(ctx, params); !errors.As(err, &pg) || pg.Code != "23514" {
				t.Fatal("incorrect inventory count sealed", err)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			// An old snapshot sees no receipt, but the shared epoch must still fence its
			// attempted append after another transaction seals the inventory.
			stale, err := runtime.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
			if err != nil {
				t.Fatal(err)
			}
			defer stale.Rollback(ctx)
			if err := ledgerdb.New(stale).BindTenant(ctx, tenant); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := stale.QueryRow(ctx, "SELECT count(*) FROM extraction_attempt_inventories WHERE tenant_id=$1 AND attempt_id=$2", tenant, attempt).Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
			params.FactCount = facts
			if err := ledgerdb.New(admin).InsertExtractionInputInventory(ctx, params); err != nil {
				t.Fatal(err)
			}
			if err := insert(stale); !errors.As(err, &pg) || pg.Code != "40001" {
				t.Fatal("stale writer bypassed inventory completion", err)
			}
			if err := stale.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			var actualFacts, actualMemories int
			if err := admin.QueryRow(ctx, `SELECT
    (SELECT count(*) FROM extraction_attempt_fact_inputs WHERE tenant_id=$1 AND attempt_id=$2),
    (SELECT count(*) FROM extraction_attempt_memory_inputs WHERE tenant_id=$1 AND attempt_id=$2)`, tenant, attempt).Scan(&actualFacts, &actualMemories); err != nil || actualFacts != int(facts) || actualMemories != int(memories) {
				t.Fatal("reference rows changed", actualFacts, actualMemories, err)
			}
		})
	}
	// Guard state is tenant-isolated and is not deletable by runtime credentials.
	tx, err := runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledgerdb.New(tx).BindTenant(ctx, "other-inventory-seal-tenant"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM extraction_inventory_guards").Scan(&count); err != nil || count != 0 {
		t.Fatal("guard tenant leak", count, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"DELETE FROM extraction_inventory_guards", "TRUNCATE extraction_inventory_guards"} {
		_, err := runtime.Exec(ctx, query)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Fatal("runtime removed inventory guard", err)
		}
	}
}
