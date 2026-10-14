package postgres_test

import (
	"bytes"
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"github.com/caxqueiroz/facts_engine/adapters/archive"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/internal/workerdaemon"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

func testStandaloneArchive(t *testing.T, ctx context.Context, client *http.Client, base, dsn, endpoint, storeID, modelID, tenant string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("MEMORY_WORKER_DATABASE_URL", databaseRole(t, dsn, "facts_archiver"))
	t.Setenv("MEMORY_ARCHIVE_ENCRYPTION_KEY_HEX", strings.Repeat("08", 32))
	t.Setenv("MEMORY_ARCHIVE_PRIVATE_KEY_HEX", hex.EncodeToString(private))
	config := workerdaemon.Config{Mode: "archive", Principal: "archive-worker", Archive: &workerdaemon.ArchiveConfig{Tenant: tenant, Stream: "ledger-v1", Directory: t.TempDir(), KeyID: "archive-one", TrustedKeys: map[string]string{"archive-one": hex.EncodeToString(public)}}, OpenFGAURL: endpoint, OpenFGAStore: storeID, OpenFGAModel: modelID, MaxConnections: 4, MaxAttempts: 3, LeaseSeconds: 3, PollMilliseconds: 10, MinBackoffMilliseconds: 10, MaxBackoffSeconds: 1, DeliveryTimeoutSeconds: 10}
	pool, err := pgxpool.New(ctx, databaseRole(t, dsn, "facts_runtime"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ledgerCipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, pool, ledgerCipher)
	if err != nil {
		t.Fatal(err)
	}
	archiveCipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{8}, 32))
	objects, err := archive.NewFiles(config.Archive.Directory, archiveCipher)
	if err != nil {
		t.Fatal(err)
	}
	runThrough := func() int64 {
		t.Helper()
		var revision int64
		if err := ledger.Transact(ctx, tenant, func(tx model.Tx) error { var err error; revision, err = tx.Revision(ctx); return err }); err != nil {
			t.Fatal(err)
		}
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- workerdaemon.Run(runCtx, config, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
		defer func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(6 * time.Second):
				t.Error("archive worker did not stop")
			}
		}()
		deadline := time.Now().Add(10 * time.Second)
		for {
			status, err := ledger.ConsumerStatus(ctx, model.ConsumerKey{Tenant: tenant, Consumer: model.ArchiveConsumer(config.Archive.Stream)})
			if err == nil && status.AcknowledgedRevision >= revision {
				return revision
			}
			if time.Now().After(deadline) || (err == nil && status.State == "failed") {
				t.Fatalf("archive progress: %+v, %v", status, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	first := runThrough()
	head, _, err := objects.Load(ctx, tenant, archive.ManifestKey(config.Archive.Stream, first))
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]ed25519.PublicKey{"archive-one": public}
	if err := archive.VerifyChain(ctx, objects, tenant, config.Archive.Stream, head, first, 1000, keys); err != nil {
		t.Fatal(err)
	}
	// Restart with a rotated signing key and retained old public trust. Existing
	// encrypted objects remain unchanged; the next manifest extends their chain.
	nextPublic, nextPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	config.Archive.KeyID = "archive-two"
	config.Archive.TrustedKeys["archive-two"] = hex.EncodeToString(nextPublic)
	keys["archive-two"] = nextPublic
	t.Setenv("MEMORY_ARCHIVE_PRIVATE_KEY_HEX", hex.EncodeToString(nextPrivate))
	identity := memoryv1connect.NewIdentityServiceClient(connect.NewClient(connecthttp.NewTransport(client, base)))
	if _, err := identity.PutScope(ctx, &v1.PutScopeRequest{Tenant: tenant, Command: &v1.Command{Key: "archive-restart-update", Purpose: "integration"}, Definition: &v1.ScopeDefinition{Id: "case1", Label: "Updated service scope"}, ExpectedRevision: 1, Reason: "Archive restart fixture"}); err != nil {
		t.Fatal(err)
	}
	second := runThrough()
	if second != first+1 {
		t.Fatalf("unexpected archive revision: %d, %d", first, second)
	}
	retained, _, err := objects.Load(ctx, tenant, archive.ManifestKey(config.Archive.Stream, first))
	if err != nil || retained != head {
		t.Fatal("restart rewrote archived object", err)
	}
	latest, _, err := objects.Load(ctx, tenant, archive.ManifestKey(config.Archive.Stream, second))
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.VerifyChain(ctx, objects, tenant, config.Archive.Stream, latest, second, 1000, keys); err != nil {
		t.Fatal(err)
	}
	if err := archive.VerifyChain(ctx, objects, tenant, config.Archive.Stream, latest, second, 1000, map[string]ed25519.PublicKey{"archive-two": nextPublic}); err == nil {
		t.Fatal("historical signing trust silently discarded")
	}
}
