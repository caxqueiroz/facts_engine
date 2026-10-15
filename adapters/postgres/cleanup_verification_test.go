package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testCleanupVerificationMigration(t *testing.T, ctx context.Context, admin *pgxpool.Pool, migration string, legacy legacyVersionPayloads) {
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	raw, _ := json.Marshal(model.VersionTombstone(legacy.factVersion))
	sealed, err := cipher.Seal("legacy", raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, acknowledged := range []bool{false, true} {
		tx, err := admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES('legacy',$1,3)", model.ProjectionConsumer("frozen-migration")); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO projection_generations(tenant_id,generation,state,fence) VALUES('legacy','frozen-migration','building',1)"); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO fact_version_disposals(tenant_id,version_id,payload) VALUES('legacy',$1,$2)", legacy.factVersion.ID, sealed); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, "UPDATE projection_generations SET state='verified' WHERE tenant_id='legacy' AND generation='frozen-migration'"); err != nil {
			t.Fatal(err)
		}
		if acknowledged {
			if _, err = tx.Exec(ctx, "INSERT INTO projection_cleanup_acknowledgements(tenant_id,kind,version_id,generation,acknowledgement_id) VALUES('legacy','fact',$1,'frozen-migration','fixture')", legacy.factVersion.ID); err != nil {
				t.Fatal(err)
			}
		}
		_, err = tx.Exec(ctx, strings.Split(migration, "-- +goose Down")[0])
		if acknowledged {
			if err != nil {
				t.Fatal("acknowledged migration rejected", err)
			}
		} else {
			var pg *pgconn.PgError
			if !errors.As(err, &pg) || pg.Code != "23514" {
				t.Fatal("migration grandfathered stale proof", err)
			}
		}
		if err = tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// Owner-created content and acknowledgements isolate the database protocol;
// these fixtures do not authorize real disposal or assert physical index cleanup.
func testCleanupVerificationRace(t *testing.T, ctx context.Context, admin *pgxpool.Pool, rebuild *postgres.Ledger, key model.GenerationKey, actor model.RebuildActor, original model.Version, cipher *postgres.AESGCM) {
	t.Helper()
	version := original
	version.ID = "cleanup-frozen-version"
	raw, _ := json.Marshal(version)
	payload, err := cipher.Seal(key.Tenant, raw)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(model.VersionTombstone(version))
	tombstone, err := cipher.Seal(key.Tenant, raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO record_versions(tenant_id,record_id,id,scope_id,predicate,schema_version,digest) SELECT tenant_id,record_id,$3,scope_id,predicate,schema_version,digest FROM record_versions WHERE tenant_id=$1 AND id=$2", key.Tenant, original.ID, version.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO fact_version_payloads(tenant_id,version_id,payload) VALUES($1,$2,$3)", key.Tenant, version.ID, payload); err != nil {
		t.Fatal(err)
	}
	// Only this generation is ready; other generations may still need cleanup.
	if _, err = admin.Exec(ctx, "INSERT INTO projection_cleanup_acknowledgements(tenant_id,kind,version_id,generation,acknowledgement_id) SELECT tenant_id,kind,version_id,generation,'ready-fixture' FROM projection_cleanup_targets WHERE tenant_id=$1 AND generation=$2 ON CONFLICT DO NOTHING", key.Tenant, key.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "UPDATE consumer_offsets SET acknowledged_revision=(SELECT revision FROM tenant_revisions WHERE tenant_id=$1) WHERE tenant_id=$1 AND consumer_id=$2", key.Tenant, model.ProjectionConsumer(key.Generation)); err != nil {
		t.Fatal(err)
	}
	stale, err := admin.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	var state string
	if err = stale.QueryRow(ctx, "SELECT state FROM projection_generations WHERE tenant_id=$1 AND generation=$2", key.Tenant, key.Generation).Scan(&state); err != nil || state != "building" {
		t.Fatal(state, err)
	}
	frozen, err := rebuild.FreezeRebuild(ctx, key, actor)
	if err != nil || frozen.State != "paused" {
		t.Fatal("acknowledged cleanup still blocks freeze", frozen, err)
	}
	_, err = stale.Exec(ctx, "INSERT INTO fact_version_disposals(tenant_id,version_id,payload) VALUES($1,$2,$3)", key.Tenant, version.ID, tombstone)
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "40001" {
		t.Fatal("stale disposal bypassed freeze", err)
	}
	if err = stale.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"paused", "verified", "activating"} {
		if _, err = admin.Exec(ctx, "UPDATE projection_generations SET state=$3 WHERE tenant_id=$1 AND generation=$2", key.Tenant, key.Generation, state); err != nil {
			t.Fatal(err)
		}
		_, err = admin.Exec(ctx, "INSERT INTO fact_version_disposals(tenant_id,version_id,payload) VALUES($1,$2,$3)", key.Tenant, version.ID, tombstone)
		if !errors.As(err, &pg) || pg.Code != "23514" {
			t.Fatal("frozen proof allowed disposal", state, err)
		}
	}
	if _, err = admin.Exec(ctx, "UPDATE projection_generations SET state='paused' WHERE tenant_id=$1 AND generation=$2", key.Tenant, key.Generation); err != nil {
		t.Fatal(err)
	}
	resumed, err := rebuild.ResumeRebuild(ctx, key, actor, frozen.Fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rebuild.FinishResume(ctx, key, actor, resumed.Fence); err != nil {
		t.Fatal(err)
	}
	stale, err = admin.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	if err = stale.QueryRow(ctx, "SELECT state FROM projection_generations WHERE tenant_id=$1 AND generation=$2", key.Tenant, key.Generation).Scan(&state); err != nil {
		t.Fatal(err)
	}
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO fact_version_disposals(tenant_id,version_id,payload) VALUES($1,$2,$3)", key.Tenant, version.ID, tombstone); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "DELETE FROM fact_version_payloads WHERE tenant_id=$1 AND version_id=$2", key.Tenant, version.ID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = stale.Exec(ctx, "UPDATE projection_generations SET state='paused' WHERE tenant_id=$1 AND generation=$2", key.Tenant, key.Generation)
	if !errors.As(err, &pg) || pg.Code != "40001" {
		t.Fatal("stale verification bypassed cleanup", err)
	}
	if err = stale.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = rebuild.FreezeRebuild(ctx, key, actor); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("new cleanup did not block freeze", err)
	}
}
