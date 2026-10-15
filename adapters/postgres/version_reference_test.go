package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDisposedVersionReferences(t *testing.T, ctx context.Context, runtime *pgxpool.Pool, tenant, kind, version string) {
	t.Helper()
	refs := []struct{ table, column string }{}
	if kind == "fact" {
		for _, table := range []string{"fact_snapshot_items", "context_items", "decision_items", "attestations", "memory_dependencies", "extraction_memory_dependencies"} {
			refs = append(refs, struct{ table, column string }{table, "version_id"})
		}
	} else {
		for _, table := range []string{"context_memory_items", "decision_memory_items", "memory_attestations"} {
			refs = append(refs, struct{ table, column string }{table, "version_id"})
		}
		for _, table := range []string{"memory_dependencies", "extraction_jobs"} {
			refs = append(refs, struct{ table, column string }{table, "memory_version_id"})
		}
	}
	refs = append(refs, struct{ table, column string }{"extraction_attempt_" + kind + "_inputs", "version_id"})
	for _, ref := range refs {
		t.Run(ref.table+"-"+ref.column, func(t *testing.T) {
			tx, err := runtime.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if err := ledgerdb.New(tx).BindTenant(ctx, tenant); err != nil {
				t.Fatal(err)
			}
			// BEFORE triggers must reject the disposed source before other row-shape
			// constraints. The full integration workflows exercise valid live inserts.
			query := "INSERT INTO " + ref.table + "(tenant_id," + ref.column + ") VALUES($1,$2)"
			if ref.table == "extraction_attempt_"+kind+"_inputs" {
				query = "INSERT INTO " + ref.table + "(tenant_id,version_id,payload_retained) VALUES($1,$2,true)"
			}
			_, err = tx.Exec(ctx, query, tenant, version)
			var pg *pgconn.PgError
			if !errors.As(err, &pg) || pg.Code != "23514" {
				t.Fatal("disposed reference was not fenced", err)
			}
		})
	}
}
