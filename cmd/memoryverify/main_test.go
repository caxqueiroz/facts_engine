package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/archive"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
)

func fixture(t *testing.T) (config, model.ObjectRef) {
	t.Helper()
	key := bytes.Repeat([]byte{7}, 32)
	t.Setenv("MEMORY_ARCHIVE_ENCRYPTION_KEY_HEX", hex.EncodeToString(key))
	cipher, err := postgres.NewAESGCM(key)
	if err != nil {
		t.Fatal(err)
	}
	c := config{Directory: t.TempDir(), Tenant: "bank", Stream: "events", ExpectedRevision: 2, MaxBatches: 2, TimeoutSeconds: 10, TrustedKeys: map[string]string{}}
	store, err := archive.NewFiles(c.Directory, cipher)
	if err != nil {
		t.Fatal(err)
	}
	var previous *model.ObjectRef
	var first model.ObjectRef
	for i, id := range []string{"key1", "key2"} {
		private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(i + 1)}, 32))
		c.TrustedKeys[id] = hex.EncodeToString(private.Public().(ed25519.PublicKey))
		revision := int64(i + 1)
		payload, err := json.Marshal(model.ArchivePayload{Event: model.Event{ID: "event" + id, Caller: model.Caller{Tenant: c.Tenant}, LedgerRevision: revision}})
		if err != nil {
			t.Fatal(err)
		}
		ref, err := store.Put(context.Background(), c.Tenant, archive.PayloadKey(c.Stream, revision), payload)
		if err != nil {
			t.Fatal(err)
		}
		m := model.ArchiveManifest{Schema: "facts-event-archive-v1", Encoding: "json-utf8-v1", Tenant: c.Tenant, Stream: c.Stream, FromRevision: revision, ThroughRevision: revision, EventCount: 1, Payload: ref, Previous: previous}
		canonical, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := json.Marshal(model.ArchiveBatch{Manifest: m, Signature: model.AuditSignature{Algorithm: "Ed25519", KeyID: id, Bytes: ed25519.Sign(private, canonical)}})
		if err != nil {
			t.Fatal(err)
		}
		head, err := store.Put(context.Background(), c.Tenant, archive.ManifestKey(c.Stream, revision), batch)
		if err != nil {
			t.Fatal(err)
		}
		previous = &head
		if i == 0 {
			first = head
		}
		c.Head = head
	}
	return c, first
}

func invoke(t *testing.T, ctx context.Context, c config) (int, string, string) {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	code := run(ctx, []string{"-config", path}, &out, &stderr)
	return code, out.String(), stderr.String()
}

func TestVerifyLocalArchive(t *testing.T) {
	c, first := fixture(t)
	code, out, stderr := invoke(t, context.Background(), c)
	if code != 0 || stderr != "" {
		t.Fatal(code, stderr)
	}
	var r report
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		t.Fatal(err)
	}
	if !r.Verified || r.Head != c.Head || r.ThroughRevision != 2 || r.ConfigurationDigest != model.Digest(c) {
		t.Fatalf("unexpected report: %+v", r)
	}
	for _, tc := range []struct {
		name   string
		change func(*config)
	}{
		{"tail truncation", func(c *config) { c.Head = first }},
		{"wrong tenant", func(c *config) { c.Tenant = "other" }},
		{"wrong stream", func(c *config) { c.Stream = "other" }},
		{"missing old trust key", func(c *config) { c.TrustedKeys = map[string]string{"key2": c.TrustedKeys["key2"]} }},
		{"batch bound", func(c *config) { c.MaxBatches = 1 }},
		{"zero timeout", func(c *config) { c.TimeoutSeconds = 0 }},
		{"wrong digest", func(c *config) { c.Head.Digest = strings.Repeat("0", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			altered := c
			tc.change(&altered)
			code, out, _ := invoke(t, context.Background(), altered)
			if code != 1 || out != "" {
				t.Fatal("invalid chain reported success", code, out)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if code, out, _ := invoke(t, ctx, c); code != 1 || out != "" {
		t.Fatal("cancellation reported success")
	}
	if err := os.Remove(filepath.Join(c.Directory, first.Locator)); err != nil {
		t.Fatal(err)
	}
	if code, out, _ := invoke(t, context.Background(), c); code != 1 || out != "" {
		t.Fatal("missing batch reported success")
	}
}

func TestVerifyFailureOutput(t *testing.T) {
	c, _ := fixture(t)
	t.Setenv("MEMORY_ARCHIVE_ENCRYPTION_KEY_HEX", strings.Repeat("a", 64))
	code, out, stderr := invoke(t, context.Background(), c)
	if code != 1 || out != "" || stderr != "archive verification failed or interrupted\n" {
		t.Fatal(code, out, stderr)
	}
	var stdout, errors bytes.Buffer
	if run(context.Background(), nil, &stdout, &errors) != 2 {
		t.Fatal("usage status")
	}
	if run(context.Background(), []string{"-config", "/private-configuration-does-not-exist"}, &stdout, &errors) != 1 {
		t.Fatal("missing configuration status")
	}
	if strings.Contains(errors.String(), "private-configuration") {
		t.Fatal("path disclosed")
	}
}

type brokenOutput struct{}

func (brokenOutput) Write([]byte) (int, error) { return 0, errors.New("private writer detail") }

func TestVerifyTamperAndOutputFailure(t *testing.T) {
	c, _ := fixture(t)
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	if code := run(context.Background(), []string{"-config", path}, brokenOutput{}, &stderr); code != 1 || stderr.String() != "cannot write verification report\n" {
		t.Fatal(code, stderr.String())
	}
	object := filepath.Join(c.Directory, c.Head.Locator)
	encrypted, err := os.ReadFile(object)
	if err != nil {
		t.Fatal(err)
	}
	encrypted[len(encrypted)-1] ^= 1
	if err := os.WriteFile(object, encrypted, 0600); err != nil {
		t.Fatal(err)
	}
	if code, out, _ := invoke(t, context.Background(), c); code != 1 || out != "" {
		t.Fatal("tampered ciphertext reported success")
	}
}
