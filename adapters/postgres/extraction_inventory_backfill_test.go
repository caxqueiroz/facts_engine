package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testExtractionInventoryBackfill(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool) {
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	var tenant string
	if err := admin.QueryRow(ctx, `SELECT a.tenant_id FROM extraction_attempts a WHERE EXISTS(SELECT 1 FROM extraction_attempt_fact_inputs f WHERE f.tenant_id=a.tenant_id) GROUP BY a.tenant_id HAVING count(*)>1 ORDER BY count(*) DESC,a.tenant_id LIMIT 1`).Scan(&tenant); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		t.Helper()
		rows, err := admin.Query(ctx, `SELECT kind,attempt_id,content FROM (
   SELECT 'receipt' AS kind,attempt_id,row_to_json(i)::text AS content FROM extraction_attempt_inventories i WHERE tenant_id=$1
   UNION ALL SELECT 'fact',attempt_id,row_to_json(f)::text FROM extraction_attempt_fact_inputs f WHERE tenant_id=$1
   UNION ALL SELECT 'memory',attempt_id,row_to_json(m)::text FROM extraction_attempt_memory_inputs m WHERE tenant_id=$1
  ) all_rows ORDER BY kind,attempt_id,content`, tenant)
		if err != nil {
			t.Fatal(err)
		}
		values := []string{}
		for rows.Next() {
			var kind, id, content string
			if err := rows.Scan(&kind, &id, &content); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			values = append(values, kind, id, content)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		return model.Digest(values)
	}
	expected := snapshot()
	clear := func() {
		t.Helper()
		for _, table := range []string{"extraction_attempt_inventories", "extraction_attempt_fact_inputs", "extraction_attempt_memory_inputs"} {
			if _, err := admin.Exec(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1", tenant); err != nil {
				t.Fatal(err)
			}
		}
	}
	count := func() int {
		t.Helper()
		var n int
		if err := admin.QueryRow(ctx, "SELECT count(*) FROM extraction_attempt_inventories WHERE tenant_id=$1", tenant).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	originalCount := count()
	var last, record, version string
	var original []byte
	if err := admin.QueryRow(ctx, `SELECT attempt_id,payload FROM extraction_attempt_payloads WHERE tenant_id=$1 ORDER BY attempt_id COLLATE "C" DESC LIMIT 1`, tenant).Scan(&last, &original); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, `SELECT record_id,version_id FROM extraction_attempt_fact_inputs WHERE tenant_id=$1 LIMIT 1`, tenant).Scan(&record, &version); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, 101} {
		if got, err := ledger.BackfillExtractionInputInventories(ctx, tenant, limit); !errors.Is(err, model.ErrInvalid) || got != (postgres.ExtractionInventoryBackfill{}) {
			t.Fatal("invalid page accepted", got, err)
		}
	}
	clear()
	// Reject an incomplete previous attempt without filling around it or leaving
	// earlier rows in the same page committed.
	if _, err := admin.Exec(ctx, `INSERT INTO extraction_attempt_fact_inputs(tenant_id,attempt_id,record_id,version_id,payload_retained) VALUES($1,$2,$3,$4,false)`, tenant, last, record, version); err != nil {
		t.Fatal(err)
	}
	if got, err := ledger.BackfillExtractionInputInventories(ctx, tenant, 100); !errors.Is(err, model.ErrPrecondition) || got != (postgres.ExtractionInventoryBackfill{}) || count() != 0 {
		t.Fatal("partial inventory not rolled back", got, err)
	}
	clear()
	// Invalid retained bytes must never produce an inventory receipt.
	plain, err := cipher.Open(tenant, original)
	if err != nil {
		t.Fatal(err)
	}
	var attempt model.ExtractionAttempt
	if err := json.Unmarshal(plain, &attempt); err != nil {
		t.Fatal(err)
	}
	attempt.RequestDigest = "corrupt"
	bad, _ := json.Marshal(attempt)
	sealed, err := cipher.Seal(tenant, bad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "UPDATE extraction_attempt_payloads SET payload=$3 WHERE tenant_id=$1 AND attempt_id=$2", tenant, last, sealed); err != nil {
		t.Fatal(err)
	}
	if got, err := ledger.BackfillExtractionInputInventories(ctx, tenant, 100); !errors.Is(err, model.ErrInvalid) || got != (postgres.ExtractionInventoryBackfill{}) || count() != 0 {
		t.Fatal("corrupt request accepted", got, err)
	}
	if _, err := admin.Exec(ctx, "UPDATE extraction_attempt_payloads SET payload=$3 WHERE tenant_id=$1 AND attempt_id=$2", tenant, last, original); err != nil {
		t.Fatal(err)
	}
	if got, err := ledger.BackfillExtractionInputInventories(ctx, "other-backfill-tenant", 1); err != nil || !got.Complete || got.Inventoried != 0 || count() != 0 {
		t.Fatal("tenant boundary failed", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if got, err := ledger.BackfillExtractionInputInventories(canceled, tenant, 1); !errors.Is(err, context.Canceled) || got != (postgres.ExtractionInventoryBackfill{}) || count() != 0 {
		t.Fatal("canceled page changed inventory", got, err)
	}
	// Two simultaneous maintenance callers serialize without duplicate rows or
	// stale repeatable-read snapshots. Each page is bounded independently.
	type result struct {
		page postgres.ExtractionInventoryBackfill
		err  error
	}
	done := make(chan result, 2)
	for range 2 {
		go func() {
			page, err := ledger.BackfillExtractionInputInventories(ctx, tenant, 1)
			done <- result{page, err}
		}()
	}
	for range 2 {
		r := <-done
		if r.err != nil || r.page.Inventoried != 1 {
			t.Fatal("concurrent backfill", r)
		}
	}
	if count() != 2 {
		t.Fatal("concurrent pages overlapped")
	}
	for calls := 0; calls <= originalCount; calls++ {
		page, err := ledger.BackfillExtractionInputInventories(ctx, tenant, 1)
		if err != nil || page.Inventoried > 1 {
			t.Fatal(page, err)
		}
		if page.Complete {
			break
		}
		if calls == originalCount {
			t.Fatal("backfill did not drain")
		}
	}
	if count() != originalCount || snapshot() != expected {
		t.Fatal("backfill changed inventory content")
	}
	again, err := ledger.BackfillExtractionInputInventories(ctx, tenant, 100)
	if err != nil || !again.Complete || again.Inventoried != 0 || snapshot() != expected {
		t.Fatal("completed page replay changed inventory", again, err)
	}
	var retained []byte
	if err := admin.QueryRow(ctx, "SELECT payload FROM extraction_attempt_payloads WHERE tenant_id=$1 AND attempt_id=$2", tenant, last).Scan(&retained); err != nil || !bytes.Equal(retained, original) {
		t.Fatal("backfill rewrote retained artifact", err)
	}
}
