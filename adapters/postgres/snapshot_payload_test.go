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

func seedSnapshotPayloadMigration(t *testing.T, ctx context.Context, admin *pgxpool.Pool) map[string][]byte {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	out := map[string][]byte{}
	for _, id := range []string{"legacy-snapshot-a", "legacy-snapshot-b"} {
		s := model.FactSnapshot{ID: id, Tenant: "legacy", Caller: model.Caller{Tenant: "legacy", Principal: "legacy-caller"}, Purpose: "migration", SelectionRevision: 1, CreationRevision: 2, CanonicalVersion: "fact-snapshot-v1", Items: []model.SnapshotItem{}}
		s.Digest = model.SnapshotDigest(s)
		plain, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		b, err := cipher.Seal("legacy", plain)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, "INSERT INTO fact_snapshots(tenant_id,id,selection_revision,creation_revision,digest,payload) VALUES('legacy',$1,1,2,$2,$3)", id, s.Digest, b); err != nil {
			t.Fatal(err)
		}
		out[id] = b
	}
	return out
}
func testSnapshotPayloadMigration(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool, want map[string][]byte) {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	for id, expected := range want {
		var b []byte
		if err := admin.QueryRow(ctx, "SELECT payload FROM fact_snapshot_payloads WHERE tenant_id='legacy' AND snapshot_id=$1", id).Scan(&b); err != nil || !bytes.Equal(b, expected) {
			t.Fatal("migration changed ciphertext", id, err)
		}
		if err := ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
			s, err := tx.(model.TemporalTx).Snapshot(ctx, id)
			if err != nil {
				return err
			}
			if s.ID != id || model.SnapshotDigest(s) != s.Digest {
				t.Fatal("migration changed snapshot", s)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='fact_snapshots' AND column_name='payload'").Scan(&count); err != nil || count != 0 {
		t.Fatal("structural row still contains payload", count, err)
	}
	tx, err := runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := ledgerdb.New(tx).BindTenant(ctx, "other-tenant"); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM fact_snapshot_payloads WHERE tenant_id='legacy'").Scan(&count); err != nil || count != 0 {
		t.Fatal("snapshot content tenant leak", count, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"DELETE FROM fact_snapshot_payloads", "UPDATE fact_snapshot_payloads SET payload=decode('00','hex')", "TRUNCATE fact_snapshot_payloads"} {
		_, err := runtime.Exec(ctx, query)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Fatal("runtime rewrote content", err)
		}
	}
	// Missing bytes must not hide a structural snapshot in an audit/archive scan.
	id := "legacy-snapshot-a"
	if _, err := admin.Exec(ctx, "DELETE FROM fact_snapshot_payloads WHERE tenant_id='legacy' AND snapshot_id=$1", id); err != nil {
		t.Fatal(err)
	}
	err = ledger.Transact(ctx, "legacy", func(tx model.Tx) error { _, err := tx.(model.TemporalTx).Snapshot(ctx, id); return err })
	if !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("missing payload masked", err)
	}

	rows, err := admin.Query(ctx, ledgerdb.ListArchiveSnapshot, "legacy", int64(2))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for rows.Next() {
		var snapshotID string
		var payload []byte
		if err := rows.Scan(&snapshotID, &payload); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if snapshotID == id {
			found = true
			if payload != nil {
				rows.Close()
				t.Fatal("unexpected missing snapshot bytes")
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil || !found {
		t.Fatal("archive scan hid missing content", found, err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO fact_snapshot_payloads(tenant_id,snapshot_id,payload) VALUES('legacy',$1,$2)", id, want[id]); err != nil {
		t.Fatal(err)
	}
}
