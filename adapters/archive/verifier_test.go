package archive_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/archive"
	"github.com/caxqueiroz/facts_engine/model"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type uncheckedArchive struct {
	model.ArchiveStore
	head model.ObjectRef
	mode string
}

func (s uncheckedArchive) Read(ctx context.Context, tenant string, ref model.ObjectRef) ([]byte, error) {
	if s.mode == "unavailable" {
		return nil, errors.New("PRIVATE-storage-error")
	}
	if s.mode == "missing" {
		return nil, model.ErrNotFound
	}
	raw, err := s.ArchiveStore.Read(ctx, tenant, ref)
	if err != nil {
		return nil, err
	}
	if (s.mode == "manifest" && ref == s.head) || (s.mode == "payload" && ref != s.head) {
		// Still valid JSON with the same event/manifest contents, but not the bytes
		// bound by the independently retained head or the signed payload reference.
		raw = append(raw, ' ')
	}
	if s.mode == "payload_digest" && ref != s.head {
		raw = bytes.ReplaceAll(raw, []byte("scope1"), []byte("scope9"))
	}
	return raw, nil
}

func testObservedArchiveVerification(t *testing.T, store model.ArchiveStore, head model.ObjectRef, keys map[string]ed25519.PublicKey) {
	t.Helper()
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(ctx) }()
	copied := map[string]ed25519.PublicKey{}
	for id, key := range keys {
		copied[id] = append(ed25519.PublicKey(nil), key...)
	}
	verifier, err := archive.NewChainVerifier(copied, model.TelemetryOptions{MeterProvider: provider})
	if err != nil {
		t.Fatal(err)
	}
	for id, key := range copied {
		key[0] ^= 1
		delete(copied, id)
	}
	var want int64
	for _, test := range []struct {
		mode            string
		expected, limit int
		failure         bool
		integrity       bool
	}{
		{"valid", 2, 2, false, false},
		{"manifest", 2, 2, true, true},
		{"payload", 2, 2, true, true},
		{"payload_digest", 2, 2, true, true},
		{"missing", 2, 2, true, true},
		{"unavailable", 2, 2, true, false},
		{"valid", 3, 3, true, true},
		{"valid", 3, 2, true, false},
	} {
		wrapped := uncheckedArchive{ArchiveStore: store, head: head, mode: test.mode}
		err := verifier.Verify(ctx, wrapped, "org", "stream1", head, int64(test.expected), test.limit)
		if (err != nil) != test.failure {
			t.Fatal(test.mode, err)
		}
		if test.integrity {
			want++
		}
		if test.mode == "manifest" || test.mode == "payload" || test.mode == "payload_digest" {
			if err := archive.VerifyChain(ctx, wrapped, "org", "stream1", head, 2, 2, keys); err == nil {
				t.Fatal("unchecked adapter bytes accepted")
			}
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := verifier.Verify(canceled, store, "org", "stream1", head, 2, 2); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &data); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "facts.export.integrity_failures" {
				for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
					total += point.Value
					if point.Attributes.Len() != 1 {
						t.Fatal("unexpected archive labels")
					}
				}
			}
		}
	}
	if total != want {
		t.Fatal(total, want)
	}
	raw, _ := json.Marshal(data)
	if strings.Contains(string(raw), "PRIVATE") || strings.Contains(string(raw), "stream1") {
		t.Fatal("private telemetry")
	}
}
