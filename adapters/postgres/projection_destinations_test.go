package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const destinationTenant = "destination-inventory"

func seedProjectionDestinations(t *testing.T, ctx context.Context, admin *pgxpool.Pool) {
	t.Helper()
	for _, generation := range []string{"Legacy.Managed-1", "legacy-static"} {
		if _, err := admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts,acknowledged_revision) VALUES($1,$2,3,17)", destinationTenant, model.ProjectionConsumer(generation)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.Exec(ctx, "INSERT INTO projection_generations(tenant_id,generation,state,fence) VALUES($1,'Legacy.Managed-1','retired',1)", destinationTenant); err != nil {
		t.Fatal(err)
	}
}

func testProjectionDestinations(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool) {
	cfg := admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["role"] = "facts_rebuild"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	inventory := func() model.ProjectionDestinationInventory {
		t.Helper()
		v, err := ledger.ProjectionDestinationInventory(ctx, destinationTenant)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	initial := inventory()
	if len(initial.Destinations) != 1 || initial.Destinations[0].Generation != "Legacy.Managed-1" || !reflect.DeepEqual(initial.MissingConsumers, []string{model.ProjectionConsumer("legacy-static")}) {
		t.Fatal("legacy mapping lost", initial)
	}
	static := model.GenerationKey{Tenant: destinationTenant, Generation: "legacy-static"}
	before, err := ledger.ConsumerStatus(ctx, model.ConsumerKey{Tenant: destinationTenant, Consumer: model.ProjectionConsumer(static.Generation)})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	for range 2 {
		go func() { results <- ledger.RegisterProjectionDestination(ctx, static) }()
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	after, err := ledger.ConsumerStatus(ctx, before.Key)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("registration reset consumer", before, after, err)
	}
	complete := inventory()
	if len(complete.Destinations) != 2 || len(complete.MissingConsumers) != 0 || complete.Epoch <= initial.Epoch {
		t.Fatal(complete)
	}
	if err := ledger.RegisterProjectionDestination(ctx, model.GenerationKey{Tenant: destinationTenant, Generation: "missing-consumer"}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("unregistered consumer accepted", err)
	}
	if err := ledger.RegisterProjectionDestination(ctx, model.GenerationKey{Tenant: destinationTenant, Generation: "invalid/name"}); !errors.Is(err, model.ErrInvalid) {
		t.Fatal(err)
	}
	// StartRebuild records a destination automatically; abandonment never removes it.
	managed := model.GenerationKey{Tenant: destinationTenant, Generation: "new-managed"}
	actor := model.RebuildActor{Principal: "operator", Purpose: "inventory"}
	started, err := ledger.StartRebuild(ctx, managed, actor, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ledger.AbandonRebuild(ctx, managed, actor, started.Fence); err != nil {
		t.Fatal(err)
	}
	v := inventory()
	if len(v.Destinations) != 3 || len(v.MissingConsumers) != 0 || v.Epoch <= complete.Epoch {
		t.Fatal("abandoned destination omitted", v)
	}
	// An old snapshot cannot lock an inventory after a new projection consumer is
	// registered, even before its generation mapping is supplied.
	stale, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Rollback(ctx)
	q := ledgerdb.New(stale)
	if err = q.BindTenant(ctx, destinationTenant); err != nil {
		t.Fatal(err)
	}
	if _, err = q.ProjectionRetentionEpoch(ctx, destinationTenant); err != nil {
		t.Fatal(err)
	}
	late := model.GenerationKey{Tenant: destinationTenant, Generation: "late"}
	if err = ledger.RegisterConsumer(ctx, model.ConsumerKey{Tenant: destinationTenant, Consumer: model.ProjectionConsumer(late.Generation)}, 3); err != nil {
		t.Fatal(err)
	}
	err = q.LockProjectionRetentionGuard(ctx, destinationTenant)
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "40001" {
		t.Fatal("stale inventory survived new destination", err)
	}
	if err = stale.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	v = inventory()
	if len(v.MissingConsumers) != 1 {
		t.Fatal("new unmapped consumer hidden", v)
	}
	if err = ledger.RegisterProjectionDestination(ctx, late); err != nil {
		t.Fatal(err)
	}
	// Immutable mapping and tenant isolation apply to the maintenance role too.
	for _, query := range []string{"UPDATE projection_destinations SET generation='changed'", "DELETE FROM projection_destinations", "TRUNCATE projection_destinations", "DELETE FROM projection_retention_guards", "TRUNCATE projection_retention_guards"} {
		_, err = pool.Exec(ctx, query)
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Fatal("inventory mutation permitted", err)
		}
	}
	_, err = runtime.Exec(ctx, "INSERT INTO projection_destinations(tenant_id,generation,consumer_id) VALUES($1,$2,$3)", destinationTenant, "late", model.ProjectionConsumer("late"))
	if !errors.As(err, &pg) || pg.Code != "42501" {
		t.Fatal("runtime registered destination", err)
	}
	other, err := ledger.ProjectionDestinationInventory(ctx, "another-tenant")
	if err != nil || len(other.Destinations) != 0 || len(other.MissingConsumers) != 0 || other.Epoch != 0 {
		t.Fatal("tenant inventory leaked", other, err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = ledgerdb.New(tx).BindTenant(ctx, destinationTenant); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "INSERT INTO projection_destinations(tenant_id,generation,consumer_id) VALUES($1,'forged',$2)", destinationTenant, model.ProjectionConsumer("late"))
	if !errors.As(err, &pg) || pg.Code != "23514" {
		t.Fatal("hash binding forged", err)
	}
}
