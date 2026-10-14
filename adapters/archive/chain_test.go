package archive_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"github.com/caxqueiroz/facts_engine/adapters/archive"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type source struct{}

func (source) ArchivePayload(_ context.Context, d model.Delivery) (model.ArchivePayload, error) {
	return model.ArchivePayload{Event: d.Event, Scopes: []string{"scope1", "scope2"}}, nil
}

type auth struct{ denied string }

func (a auth) Check(_ context.Context, r model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	return model.AuthorizationDecision{Allowed: r.Scope != a.denied, ModelID: "model", Consistency: "HIGHER_CONSISTENCY", CheckedAt: time.Now()}, nil
}
func TestArchiveChainRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cipher, err := postgres.NewAESGCM(bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	store, err := archive.NewFiles(dir, cipher)
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := audit.NewEd25519("key1", private)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]ed25519.PublicKey{"key1": public}
	c := model.Caller{Tenant: "bank", Principal: "archive-worker"}
	handler, err := workers.NewArchive(source{}, store, signer, keys, auth{}, c, "stream1")
	if err != nil {
		t.Fatal(err)
	}
	delivery := func(revision int64) model.Delivery {
		return model.Delivery{Key: model.ConsumerKey{Tenant: "bank", Consumer: model.ArchiveConsumer("stream1")}, Event: model.Event{ID: map[int64]string{1: "event1", 2: "event2"}[revision], Scope: "scope1", LedgerRevision: revision, Caller: c}}
	}
	if err := handler.Handle(ctx, delivery(2)); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("missing predecessor ignored", err)
	}
	denied, _ := workers.NewArchive(source{}, store, signer, keys, auth{denied: "scope2"}, c, "stream1")
	if err := denied.Handle(ctx, delivery(1)); !errors.Is(err, model.ErrDenied) {
		t.Fatal("secondary scope disclosed", err)
	}
	if _, _, err := store.Load(ctx, "bank", archive.ManifestKey("stream1", 1)); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("denied manifest written", err)
	}
	for _, rev := range []int64{1, 1, 2, 2} {
		if err := handler.Handle(ctx, delivery(rev)); err != nil {
			t.Fatal(err)
		}
	}
	head, raw, err := store.Load(ctx, "bank", archive.ManifestKey("stream1", 2))
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.VerifyChain(ctx, store, "bank", "stream1", head, 2, 2, keys); err != nil {
		t.Fatal(err)
	}
	testObservedArchiveVerification(t, store, head, keys)
	if err := archive.VerifyChain(ctx, store, "bank", "stream1", head, 3, 3, keys); err == nil {
		t.Fatal("truncated chain accepted")
	}
	if err := archive.VerifyChain(ctx, store, "bank", "stream1", head, 2, 2, map[string]ed25519.PublicKey{}); err == nil {
		t.Fatal("untrusted signer accepted")
	}
	if _, err := store.Read(ctx, "another-bank", head); err == nil {
		t.Fatal("cross-tenant decrypt")
	}
	if _, err := store.Put(ctx, "bank", archive.ManifestKey("stream1", 2), []byte("changed")); !errors.Is(err, model.ErrConflict) {
		t.Fatal("immutable object overwritten", err)
	}
	encrypted, err := os.ReadFile(filepath.Join(dir, head.Locator))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encrypted, raw) {
		t.Fatal("archive plaintext on disk")
	}
	altered := append([]byte(nil), encrypted...)
	altered[len(altered)-1] ^= 1
	if err := os.WriteFile(filepath.Join(dir, head.Locator), altered, 0600); err != nil {
		t.Fatal(err)
	}
	if err := archive.VerifyChain(ctx, store, "bank", "stream1", head, 2, 2, keys); err == nil {
		t.Fatal("tampering accepted")
	}
	if err := os.WriteFile(filepath.Join(dir, head.Locator), encrypted, 0600); err != nil {
		t.Fatal(err)
	}
	prev, _, err := store.Load(ctx, "bank", archive.ManifestKey("stream1", 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, prev.Locator)); err != nil {
		t.Fatal(err)
	}
	if err := archive.VerifyChain(ctx, store, "bank", "stream1", head, 2, 2, keys); err == nil {
		t.Fatal("missing batch accepted")
	}
}
