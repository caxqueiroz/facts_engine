// memoryverify verifies an encrypted local archive against an independent anchor.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caxqueiroz/facts_engine/adapters/archive"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/model"
)

type config struct {
	Directory        string            `json:"directory"`
	Tenant           string            `json:"tenant"`
	Stream           string            `json:"stream"`
	Head             model.ObjectRef   `json:"head"`
	ExpectedRevision int64             `json:"expected_revision"`
	MaxBatches       int               `json:"max_batches"`
	TimeoutSeconds   int               `json:"timeout_seconds"`
	TrustedKeys      map[string]string `json:"trusted_keys"`
}

type report struct {
	Schema              string          `json:"schema"`
	Tenant              string          `json:"tenant"`
	Stream              string          `json:"stream"`
	Head                model.ObjectRef `json:"head"`
	ThroughRevision     int64           `json:"through_revision"`
	ConfigurationDigest string          `json:"configuration_digest"`
	Verified            bool            `json:"verified"`
}

func validDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

func (c config) keys() (map[string]ed25519.PublicKey, error) {
	if !model.ValidID(c.Tenant) || !model.ValidID(c.Stream) || c.ExpectedRevision < 1 || c.MaxBatches < 1 || c.MaxBatches > 1_000_000 || c.ExpectedRevision > int64(c.MaxBatches) || c.TimeoutSeconds < 1 || c.TimeoutSeconds > 3600 || len(c.TrustedKeys) < 1 || len(c.TrustedKeys) > 32 || !validDigest(c.Head.Locator) || !validDigest(c.Head.Version) || !validDigest(c.Head.Digest) || c.Head.Size < 1 || c.Head.Size > 64<<20 {
		return nil, model.ErrInvalid
	}
	keys := make(map[string]ed25519.PublicKey, len(c.TrustedKeys))
	for id, value := range c.TrustedKeys {
		key, err := hex.DecodeString(value)
		if !model.ValidID(id) || err != nil || len(key) != ed25519.PublicKeySize {
			return nil, model.ErrInvalid
		}
		keys[id] = key
	}
	return keys, nil
}

func run(ctx context.Context, args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("memoryverify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("config", "", "verification configuration JSON path")
	if flags.Parse(args) != nil || *path == "" || flags.NArg() != 0 {
		fmt.Fprintln(errOut, "usage: memoryverify -config PATH")
		return 2
	}
	var c config
	if runtimeconfig.Read(*path, &c) != nil {
		fmt.Fprintln(errOut, "verification configuration rejected")
		return 1
	}
	keys, err := c.keys()
	if err != nil {
		fmt.Fprintln(errOut, "verification configuration rejected")
		return 1
	}
	key, err := runtimeconfig.HexKey("MEMORY_ARCHIVE_ENCRYPTION_KEY_HEX", 32)
	if err != nil {
		fmt.Fprintln(errOut, "archive encryption key rejected")
		return 1
	}
	cipher, err := postgres.NewAESGCM(key)
	if err != nil {
		fmt.Fprintln(errOut, "archive encryption key rejected")
		return 1
	}
	store, err := archive.NewFiles(c.Directory, cipher)
	if err != nil {
		fmt.Fprintln(errOut, "archive directory unavailable")
		return 1
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.TimeoutSeconds)*time.Second)
	defer cancel()
	if archive.VerifyChain(ctx, store, c.Tenant, c.Stream, c.Head, c.ExpectedRevision, c.MaxBatches, keys) != nil {
		fmt.Fprintln(errOut, "archive verification failed or interrupted")
		return 1
	}
	r := report{Schema: "facts-archive-verification-v1", Tenant: c.Tenant, Stream: c.Stream, Head: c.Head, ThroughRevision: c.ExpectedRevision, ConfigurationDigest: model.Digest(c), Verified: true}
	if json.NewEncoder(out).Encode(r) != nil {
		fmt.Fprintln(errOut, "cannot write verification report")
		return 1
	}
	return 0
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
