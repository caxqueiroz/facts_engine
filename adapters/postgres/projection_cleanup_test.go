package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testProjectionCleanupMigration(t *testing.T, ctx context.Context, admin *pgxpool.Pool, migration string, legacy legacyVersionPayloads) {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	for _, missing := range []bool{false, true} {
		tx, err := admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		key := model.GenerationKey{Tenant: "legacy", Generation: "cleanup-legacy"}
		if _, err = tx.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,$2,3)", key.Tenant, model.ProjectionConsumer(key.Generation)); err != nil {
			t.Fatal(err)
		}
		if !missing {
			if _, err = tx.Exec(ctx, "INSERT INTO projection_destinations(tenant_id,generation,consumer_id) VALUES($1,$2,$3)", key.Tenant, key.Generation, model.ProjectionConsumer(key.Generation)); err != nil {
				t.Fatal(err)
			}
		}
		for _, item := range []struct {
			kind, id string
			value    any
		}{{"fact", legacy.factVersion.ID, model.VersionTombstone(legacy.factVersion)}, {"memory", legacy.memoryVersion.ID, model.MemoryVersionTombstone(legacy.memoryVersion)}} {
			raw, _ := json.Marshal(item.value)
			sealed, err := cipher.Seal("legacy", raw)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(ctx, "INSERT INTO "+item.kind+"_version_disposals(tenant_id,version_id,payload) VALUES('legacy',$1,$2)", item.id, sealed); err != nil {
				t.Fatal(err)
			}
		}
		_, err = tx.Exec(ctx, strings.Split(migration, "-- +goose Down")[0])
		if missing {
			var pg *pgconn.PgError
			if !errors.As(err, &pg) || pg.Code != "23514" {
				t.Fatal("migration hid missing inventory", err)
			}
		} else {
			if err != nil {
				t.Fatal("cleanup migration", err)
			}
			var jobs, targets, acks int
			if err = tx.QueryRow(ctx, "SELECT (SELECT count(*) FROM projection_cleanup_jobs WHERE tenant_id='legacy'),(SELECT count(*) FROM projection_cleanup_targets WHERE tenant_id='legacy'),(SELECT count(*) FROM projection_cleanup_acknowledgements WHERE tenant_id='legacy')").Scan(&jobs, &targets, &acks); err != nil || jobs != 2 || targets != 2 || acks != 0 {
				t.Fatal("legacy backfill", jobs, targets, acks, err)
			}
		}
		if err = tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// Owner writes simulate storage/acknowledgement only, not policy authorization or
// actual index deletion. Runtime credentials cannot perform either operation.
func testProjectionCleanup(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool, legacy legacyVersionPayloads) {
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	const tenant = "legacy"
	const version = "cleanup-version"
	seal := func(v any) []byte {
		t.Helper()
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		b, err := cipher.Seal(tenant, raw)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	f, m := legacy.factVersion, legacy.memoryVersion
	f.ID = version
	m.ID = version
	if _, err = admin.Exec(ctx, "INSERT INTO record_versions(tenant_id,record_id,id,scope_id,predicate,schema_version,digest) SELECT tenant_id,record_id,$2,scope_id,predicate,schema_version,digest FROM record_versions WHERE tenant_id=$1 AND id='payload-fact'", tenant, version); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO memory_versions(tenant_id,record_id,id,scope_id,subtype,schema_version,ledger_revision) SELECT tenant_id,record_id,$2,scope_id,subtype,schema_version,ledger_revision FROM memory_versions WHERE tenant_id=$1 AND id='payload-memory'", tenant, version); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		kind  string
		value any
	}{{"fact", f}, {"memory", m}} {
		if _, err = admin.Exec(ctx, "INSERT INTO "+item.kind+"_version_payloads(tenant_id,version_id,payload) VALUES($1,$2,$3)", tenant, version, seal(item.value)); err != nil {
			t.Fatal(err)
		}
	}
	register := func(generation, state string, mapped bool) {
		t.Helper()
		consumer := model.ProjectionConsumer(generation)
		if _, err := admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,$2,3)", tenant, consumer); err != nil {
			t.Fatal(err)
		}
		if state != "" {
			if _, err := admin.Exec(ctx, "INSERT INTO projection_generations(tenant_id,generation,state,fence) VALUES($1,$2,$3,1)", tenant, generation, state); err != nil {
				t.Fatal(err)
			}
		} else if mapped {
			if _, err := admin.Exec(ctx, "INSERT INTO projection_destinations(tenant_id,generation,consumer_id) VALUES($1,$2,$3)", tenant, generation, consumer); err != nil {
				t.Fatal(err)
			}
		}
	}
	register("cleanup-retired", "retired", true)
	register("cleanup-abandoned", "abandoned", true)
	register("cleanup-static", "", false)
	tombstones := map[string][]byte{"fact": seal(model.VersionTombstone(f)), "memory": seal(model.MemoryVersionTombstone(m))}
	insert := func(tx pgx.Tx, kind string) error {
		_, err := tx.Exec(ctx, "INSERT INTO "+kind+"_version_disposals(tenant_id,version_id,payload) VALUES($1,$2,$3)", tenant, version, tombstones[kind])
		return err
	}
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = insert(tx, "fact")
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "23514" {
		t.Fatal("unmapped destination accepted", err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	key := model.ProjectionCleanupKey{Tenant: tenant, Kind: "fact", VersionID: version}
	if _, err = ledger.ProjectionCleanupStatus(ctx, key); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("failed disposal left a job", err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO projection_destinations(tenant_id,generation,consumer_id) VALUES($1,'cleanup-static',$2)", tenant, model.ProjectionConsumer("cleanup-static")); err != nil {
		t.Fatal(err)
	}
	// Roll back a successful tombstone and payload deletion: obligations must also disappear.
	tx, err = admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = insert(tx, "fact"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "DELETE FROM fact_version_payloads WHERE tenant_id=$1 AND version_id=$2", tenant, version); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = ledger.ProjectionCleanupStatus(ctx, key); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("rollback left obligations", err)
	}
	tx, err = admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = insert(tx, "fact"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "DELETE FROM fact_version_payloads WHERE tenant_id=$1 AND version_id=$2", tenant, version); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	read := func(k model.ProjectionCleanupKey) model.ProjectionCleanupStatus {
		t.Helper()
		s, err := ledger.ProjectionCleanupStatus(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	initial := read(key)
	if initial.Complete || len(initial.Targets) != 3 || initial.InventoryEpoch < 1 {
		t.Fatal(initial)
	}
	// A late registration invalidates an old disposal snapshot before it can capture targets.
	stale, err := admin.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err = stale.QueryRow(ctx, "SELECT count(*) FROM projection_destinations WHERE tenant_id=$1", tenant).Scan(&count); err != nil {
		t.Fatal(err)
	}
	register("cleanup-late", "", true)
	err = insert(stale, "memory")
	if !errors.As(err, &pg) || pg.Code != "40001" {
		t.Fatal("stale disposal lost new target", err)
	}
	if err = stale.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err = admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = insert(tx, "memory"); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "DELETE FROM memory_version_payloads WHERE tenant_id=$1 AND version_id=$2", tenant, version); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	memoryKey := key
	memoryKey.Kind = "memory"
	if s := read(memoryKey); s.Complete || len(s.Targets) != 4 || s.InventoryEpoch <= initial.InventoryEpoch {
		t.Fatal(s)
	}
	if s := read(key); len(s.Targets) != 3 {
		t.Fatal("captured target set changed", s)
	}
	for i, target := range initial.Targets {
		if _, err = admin.Exec(ctx, "INSERT INTO projection_cleanup_acknowledgements(tenant_id,kind,version_id,generation,acknowledgement_id) VALUES($1,'fact',$2,$3,$4)", tenant, version, target.Generation, fmt.Sprintf("ack-%d", i)); err != nil {
			t.Fatal(err)
		}
		s := read(key)
		if s.Complete != (i == len(initial.Targets)-1) {
			t.Fatal("completion ignores pending targets", s)
		}
	}
	if s := read(memoryKey); s.Complete {
		t.Fatal("fact acknowledgements completed memory cleanup", s)
	}
	other := key
	other.Tenant = "other-tenant"
	if _, err = ledger.ProjectionCleanupStatus(ctx, other); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("cross-tenant cleanup disclosed", err)
	}
	for _, role := range []string{"facts_runtime", "facts_worker", "facts_retention"} {
		cfg := admin.Config().Copy()
		cfg.ConnConfig.RuntimeParams["role"] = role
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"projection_cleanup_jobs", "projection_cleanup_targets", "projection_cleanup_acknowledgements"} {
			for _, query := range []string{"INSERT INTO " + table + "(tenant_id) VALUES('forged')", "UPDATE " + table + " SET tenant_id='forged'", "DELETE FROM " + table, "TRUNCATE " + table} {
				_, err = pool.Exec(ctx, query)
				if !errors.As(err, &pg) || pg.Code != "42501" {
					t.Fatal("cleanup status mutation allowed", role, query, err)
				}
			}
		}
		pool.Close()
	}
	// A missing target cannot be concealed behind a completion count.
	if _, err = admin.Exec(ctx, "DELETE FROM projection_cleanup_targets WHERE tenant_id=$1 AND kind='memory' AND version_id=$2 AND generation='cleanup-static'", tenant, version); err != nil {
		t.Fatal(err)
	}
	if _, err = ledger.ProjectionCleanupStatus(ctx, memoryKey); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("incomplete target inventory accepted", err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO projection_cleanup_targets(tenant_id,kind,version_id,generation) VALUES($1,'memory',$2,'cleanup-static')", tenant, version); err != nil {
		t.Fatal(err)
	}
}
