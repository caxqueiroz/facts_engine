package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedContextPayloadMigration(t *testing.T, ctx context.Context, admin *pgxpool.Pool) map[string][]byte {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	out := map[string][]byte{}
	for _, id := range []string{"legacy-context-a", "legacy-context-b"} {
		m := model.ContextManifest{ID: id, Tenant: "legacy", Scope: "scope", Caller: model.Caller{Tenant: "legacy", Principal: "reader"}, Request: model.ContextRequest{Command: model.Command{Key: id, Purpose: "migration"}, Scope: "scope", Query: model.RecallRequest{Scopes: []string{"scope"}, Query: "legacy lookup", Purpose: "migration", ValidAt: time.Now().UTC().Truncate(time.Microsecond), Limit: 1}}, CanonicalVersion: "context-manifest-v1", RedactionVersion: "none", QueryReference: model.BytesDigest([]byte("legacy lookup")), Rendered: []byte(`{"facts":[]}`), SelectionRevision: 1, CreationRevision: 2}
		m.RenderedDigest = model.BytesDigest(m.Rendered)
		m.Digest = model.ContextDigest(m)
		plain, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		b, err := cipher.Seal("legacy", plain)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, "INSERT INTO context_manifests(tenant_id,id,scope_id,selection_revision,ledger_revision,payload) VALUES('legacy',$1,'scope',1,2,$2)", id, b); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, "INSERT INTO context_scopes(tenant_id,context_id,scope_id) VALUES('legacy',$1,'scope')", id); err != nil {
			t.Fatal(err)
		}
		out[id] = b
	}
	return out
}
func testContextPayloadMigration(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool, want map[string][]byte) {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	for id, expected := range want {
		var b []byte
		if err := admin.QueryRow(ctx, "SELECT payload FROM context_payloads WHERE tenant_id='legacy' AND context_id=$1", id).Scan(&b); err != nil || !bytes.Equal(b, expected) {
			t.Fatal("context ciphertext changed", id, err)
		}
		if err := ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
			m, err := tx.(model.ContextTx).ContextManifest(ctx, id)
			if err != nil {
				return err
			}
			if m.ID != id || m.Digest != model.ContextDigest(m) || m.RenderedDigest != model.BytesDigest(m.Rendered) || !bytes.Equal(m.Rendered, []byte(`{"facts":[]}`)) {
				t.Fatal("context migration changed retained input", m)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='context_manifests' AND column_name='payload'").Scan(&count); err != nil || count != 0 {
		t.Fatal("structural context still stores payload", count, err)
	}
	tx, err := runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := ledgerdb.New(tx).BindTenant(ctx, "other-tenant"); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM context_payloads WHERE tenant_id='legacy'").Scan(&count); err != nil || count != 0 {
		t.Fatal("context content tenant leak", count, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"DELETE FROM context_payloads", "UPDATE context_payloads SET payload=decode('00','hex')", "TRUNCATE context_payloads"} {
		_, err := runtime.Exec(ctx, query)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Fatal("runtime modified context payload", err)
		}
	}
	id := "legacy-context-a"
	if _, err := admin.Exec(ctx, "DELETE FROM context_payloads WHERE tenant_id='legacy' AND context_id=$1", id); err != nil {
		t.Fatal(err)
	}
	err = ledger.Transact(ctx, "legacy", func(tx model.Tx) error { _, err := tx.(model.ContextTx).ContextManifest(ctx, id); return err })
	if !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("missing context bytes did not fail closed", err)
	}
	for _, query := range []struct {
		sql string
		arg any
	}{{ledgerdb.ListArchiveContext, int64(2)}, {ledgerdb.ListAuditContext, "scope"}} {
		rows, err := admin.Query(ctx, query.sql, "legacy", query.arg)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for rows.Next() {
			var contextID string
			var payload []byte
			if err := rows.Scan(&contextID, &payload); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if contextID == id {
				found = true
				if payload != nil {
					rows.Close()
					t.Fatal("unexpected content in missing context")
				}
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil || !found {
			t.Fatal("audit/archive hid missing context", found, err)
		}
	}
	if _, err := admin.Exec(ctx, "INSERT INTO context_payloads(tenant_id,context_id,payload) VALUES('legacy',$1,$2)", id, want[id]); err != nil {
		t.Fatal(err)
	}
}
