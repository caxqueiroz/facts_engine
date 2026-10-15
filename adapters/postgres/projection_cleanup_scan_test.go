package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testCleanupScan(t *testing.T, ctx context.Context, ledger *postgres.Ledger, admin *pgxpool.Pool, tenant, disposalID string) {
	t.Helper()
	q := model.ProjectionCleanupQuery{Tenant: tenant, Limit: 1}
	seen := map[model.ProjectionCleanupCursor]bool{}
	for range 5 {
		page, err := ledger.ScanProjectionCleanup(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		if len(page) != 1 {
			t.Fatal("scan exceeded limit")
		}
		r := page[0]
		next := model.ProjectionCleanupCursor{Kind: r.Kind, VersionID: r.VersionID, Generation: r.Generation}
		if seen[next] || next.Compare(q.After) <= 0 || r.Key != "" || r.Reason != "" || r.Purpose != "retention" || len(r.Bindings) == 0 {
			t.Fatal("invalid scan request", r)
		}
		seen[next] = true
		q.After = next
	}
	if len(seen) != 4 {
		t.Fatal("scan missed captured targets", seen)
	}
	if page, err := ledger.ScanProjectionCleanup(ctx, model.ProjectionCleanupQuery{Tenant: "unrelated", Limit: 100}); err != nil || len(page) != 0 {
		t.Fatal("tenant scan leaked", page, err)
	}
	if _, err := ledger.ScanProjectionCleanup(ctx, model.ProjectionCleanupQuery{Tenant: tenant, Limit: 101}); !errors.Is(err, model.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO retention_events(tenant_id,id,scope_id,kind,target_id,ledger_revision,payload) SELECT tenant_id,'duplicate-disposal',scope_id,kind,target_id,ledger_revision,payload FROM retention_events WHERE tenant_id=$1 AND id=$2", tenant, disposalID); err == nil {
		t.Fatal("duplicate disposal authorization accepted")
	} else {
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "23505" {
			t.Fatal(err)
		}
	}
	if _, err := admin.Exec(ctx, "UPDATE retention_events SET kind='hidden-fixture' WHERE tenant_id=$1 AND id=$2", tenant, disposalID); err != nil {
		t.Fatal(err)
	}
	_, scanErr := ledger.ScanProjectionCleanup(ctx, model.ProjectionCleanupQuery{Tenant: tenant, Limit: 100})
	if _, err := admin.Exec(ctx, "UPDATE retention_events SET kind='dispose_fact_payload' WHERE tenant_id=$1 AND id=$2", tenant, disposalID); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(scanErr, model.ErrPrecondition) {
		t.Fatal("missing authorization history skipped", scanErr)
	}
}
