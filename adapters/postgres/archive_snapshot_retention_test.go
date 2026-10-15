package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/adapters/archive"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Archive the real tenant stream before disposal, then return a check that
// retries the retained deliveries against the tombstoned ledger view.
func archiveSnapshotsBeforeDisposal(t *testing.T, ctx context.Context, admin *pgxpool.Pool, cipher postgres.Cipher, signer model.AuditSigner, keys map[string]ed25519.PublicKey, tenant string, snapshots ...model.FactSnapshot) func() {
	t.Helper()
	cfg := admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["role"] = "facts_archiver"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ledger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	archiveCipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{5}, 32))
	store, err := archive.NewFiles(t.TempDir(), archiveCipher)
	if err != nil {
		t.Fatal(err)
	}
	stream := "snapshot-retention-replay"
	caller := model.Caller{Tenant: tenant, Principal: "snapshot-archiver", Service: true}
	worker, err := workers.NewArchive(ledger, store, signer, keys, allow{}, caller, stream)
	if err != nil {
		t.Fatal(err)
	}
	key := model.ConsumerKey{Tenant: tenant, Consumer: model.ArchiveConsumer(stream)}
	if err := ledger.RegisterConsumer(ctx, key, 3); err != nil {
		t.Fatal(err)
	}
	var through int64
	if err := admin.QueryRow(ctx, "SELECT revision FROM tenant_revisions WHERE tenant_id=$1", tenant).Scan(&through); err != nil {
		t.Fatal(err)
	}
	deliveries := map[int64]model.Delivery{}
	for revision := int64(1); revision <= through; revision++ {
		delivery, err := ledger.ClaimDelivery(ctx, key, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if delivery.Event.LedgerRevision != revision {
			t.Fatal("archive order", delivery.Event.LedgerRevision, revision)
		}
		if err := worker.Handle(ctx, delivery); err != nil {
			t.Fatal("archive before disposal", revision, err)
		}
		if err := ledger.AcknowledgeDelivery(ctx, delivery); err != nil {
			t.Fatal(err)
		}
		for _, snapshot := range snapshots {
			if snapshot.CreationRevision == revision {
				deliveries[revision] = delivery
			}
		}
	}
	originals := map[int64][]byte{}
	for revision := range deliveries {
		_, raw, err := store.Load(ctx, tenant, archive.ManifestKey(stream, revision))
		if err != nil {
			t.Fatal(err)
		}
		originals[revision] = raw
	}
	head, _, err := store.Load(ctx, tenant, archive.ManifestKey(stream, through))
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if len(deliveries) != len(snapshots) {
			t.Fatal("snapshot deliveries missing")
		}
		for revision, delivery := range deliveries {
			if err := worker.Handle(ctx, delivery); err != nil {
				t.Fatal("archive retry after disposal", revision, err)
			}
			_, raw, err := store.Load(ctx, tenant, archive.ManifestKey(stream, revision))
			if err != nil || !bytes.Equal(raw, originals[revision]) {
				t.Fatal("retention rewrote signed archive", revision, err)
			}
		}
		if err := archive.VerifyChain(ctx, store, tenant, stream, head, through, int(through), keys); err != nil {
			t.Fatal("retained archive chain", err)
		}
	}
}
