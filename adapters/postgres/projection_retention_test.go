package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDisposedProjectionReads(t *testing.T, ctx context.Context, admin *pgxpool.Pool, cipher *postgres.AESGCM, tenant string, factRevision, memoryRevision int64, fact model.Version, memory model.MemoryVersion) {
	t.Helper()
	cfg := admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["role"] = "facts_rebuild"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ledger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	key := model.GenerationKey{Tenant: tenant, Generation: "retention-rebuild"}
	if err = ledger.RegisterConsumer(ctx, model.ConsumerKey{Tenant: tenant, Consumer: model.ProjectionConsumer(key.Generation)}, 3); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO projection_generations(tenant_id,generation,state,fence,through_revision) VALUES($1,$2,'paused',1,$3)", tenant, key.Generation, memoryRevision); err != nil {
		t.Fatal(err)
	}
	for _, target := range []struct {
		kind, id string
		revision int64
		original any
	}{{"fact", fact.ID, factRevision, fact}, {"memory", memory.ID, memoryRevision, memory}} {
		var sealed []byte
		if err = admin.QueryRow(ctx, "SELECT payload FROM ledger_events WHERE tenant_id=$1 AND ledger_revision=$2", tenant, target.revision).Scan(&sealed); err != nil {
			t.Fatal(err)
		}
		raw, err := cipher.Open(tenant, sealed)
		if err != nil {
			t.Fatal(err)
		}
		var event model.Event
		if err = json.Unmarshal(raw, &event); err != nil {
			t.Fatal(err)
		}
		delivery := model.Delivery{Key: model.ConsumerKey{Tenant: tenant}, Event: event}
		if doc, err := ledger.ProjectionDocument(ctx, delivery, key.Generation); err != nil || doc != nil {
			t.Fatal("disposed version projected", doc, err)
		}
		table := target.kind + "_version_disposals"
		var originalTombstone []byte
		if err = admin.QueryRow(ctx, "SELECT payload FROM "+table+" WHERE tenant_id=$1 AND version_id=$2", tenant, target.id).Scan(&originalTombstone); err != nil {
			t.Fatal(err)
		}
		// A row in the disposal table containing unmarked full content is corruption,
		// not permission to publish that content or to silently omit the version.
		raw, err = json.Marshal(target.original)
		if err != nil {
			t.Fatal(err)
		}
		sealed, err = cipher.Seal(tenant, raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = admin.Exec(ctx, "UPDATE "+table+" SET payload=$3 WHERE tenant_id=$1 AND version_id=$2", tenant, target.id, sealed); err != nil {
			t.Fatal(err)
		}
		if doc, err := ledger.ProjectionDocument(ctx, delivery, key.Generation); !errors.Is(err, model.ErrPrecondition) || doc != nil {
			t.Fatal("invalid disposal projected", doc, err)
		}
		if _, err = ledger.ProjectionDocuments(ctx, key, memoryRevision); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("rebuild hid corrupt disposal", err)
		}
		if _, err = admin.Exec(ctx, "UPDATE "+table+" SET payload=$3 WHERE tenant_id=$1 AND version_id=$2", tenant, target.id, originalTombstone); err != nil {
			t.Fatal(err)
		}
		tombstoneRaw, err := cipher.Open(tenant, originalTombstone)
		if err != nil {
			t.Fatal(err)
		}
		var extended map[string]json.RawMessage
		if err = json.Unmarshal(tombstoneRaw, &extended); err != nil {
			t.Fatal(err)
		}
		extended["UnknownContent"] = json.RawMessage(`"private"`)
		raw, err = json.Marshal(extended)
		if err != nil {
			t.Fatal(err)
		}
		sealed, err = cipher.Seal(tenant, raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = admin.Exec(ctx, "UPDATE "+table+" SET payload=$3 WHERE tenant_id=$1 AND version_id=$2", tenant, target.id, sealed); err != nil {
			t.Fatal(err)
		}
		if doc, err := ledger.ProjectionDocument(ctx, delivery, key.Generation); !errors.Is(err, model.ErrPrecondition) || doc != nil {
			t.Fatal("unknown retained content silently skipped", doc, err)
		}
		// A retained header with no tombstone/payload remains an integrity failure.
		// Owner-only corruption fixture removes its dependent obligation rows.
		if _, err = admin.Exec(ctx, "DELETE FROM projection_cleanup_targets WHERE tenant_id=$1 AND kind=$2 AND version_id=$3", tenant, target.kind, target.id); err != nil {
			t.Fatal(err)
		}
		if _, err = admin.Exec(ctx, "DELETE FROM projection_cleanup_jobs WHERE tenant_id=$1 AND kind=$2 AND version_id=$3", tenant, target.kind, target.id); err != nil {
			t.Fatal(err)
		}
		if _, err = admin.Exec(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1 AND version_id=$2", tenant, target.id); err != nil {
			t.Fatal(err)
		}
		if doc, err := ledger.ProjectionDocument(ctx, delivery, key.Generation); !errors.Is(err, model.ErrPrecondition) || doc != nil {
			t.Fatal("missing content silently skipped", doc, err)
		}
		marked := target.original
		switch v := marked.(type) {
		case model.Version:
			v.PayloadDisposed = true
			marked = v
		case model.MemoryVersion:
			v.PayloadDisposed = true
			marked = v
		}
		raw, err = json.Marshal(marked)
		if err != nil {
			t.Fatal(err)
		}
		sealed, err = cipher.Seal(tenant, raw)
		if err != nil {
			t.Fatal(err)
		}
		payloadTable := target.kind + "_version_payloads"
		if _, err = admin.Exec(ctx, "INSERT INTO "+payloadTable+"(tenant_id,version_id,payload) VALUES($1,$2,$3)", tenant, target.id, sealed); err != nil {
			t.Fatal(err)
		}
		if doc, err := ledger.ProjectionDocument(ctx, delivery, key.Generation); !errors.Is(err, model.ErrPrecondition) || doc != nil {
			t.Fatal("marker without disposal treated as deletion", doc, err)
		}
		if _, err = admin.Exec(ctx, "DELETE FROM "+payloadTable+" WHERE tenant_id=$1 AND version_id=$2", tenant, target.id); err != nil {
			t.Fatal(err)
		}
		if _, err = admin.Exec(ctx, "INSERT INTO "+table+"(tenant_id,version_id,payload) VALUES($1,$2,$3)", tenant, target.id, originalTombstone); err != nil {
			t.Fatal(err)
		}
	}
	docs, err := ledger.ProjectionDocuments(ctx, key, memoryRevision)
	if err != nil || len(docs) != 0 {
		t.Fatal("legacy rebuild includes disposed content", docs, err)
	}
	first, err := ledger.ProjectionDocumentPage(ctx, key, 1, memoryRevision, model.ProjectionCursor{}, 1)
	if err != nil || len(first.Documents) != 0 || first.Next == nil || first.Next.Kind != "fact" || first.Next.VersionID != fact.ID {
		t.Fatal("disposed-only page cannot advance", first, err)
	}
	second, err := ledger.ProjectionDocumentPage(ctx, key, 1, memoryRevision, *first.Next, 1)
	if err != nil || len(second.Documents) != 0 || second.Next != nil {
		t.Fatal("disposed scan did not terminate", second, err)
	}
}
