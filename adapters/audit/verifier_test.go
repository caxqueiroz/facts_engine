package audit

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestObservedVerifierIntegrityAndTrustIsolation(t *testing.T) {
	ctx := context.Background()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewEd25519("trusted", private)
	if err != nil {
		t.Fatal(err)
	}
	entry := model.AuditEntry{Kind: "version", ID: "PRIVATE-record", Payload: json.RawMessage(`{"value":"PRIVATE-content"}`)}
	manifest := model.AuditManifest{SchemaVersion: "facts-audit-v1", Encoding: "json-utf8-v1", Tenant: "PRIVATE-tenant", EntryCount: 1, Entries: []model.AuditManifestEntry{{Kind: entry.Kind, ID: entry.ID, Digest: model.BytesDigest(entry.Payload)}}}
	raw, _ := json.Marshal(manifest)
	signature, err := signer.Sign(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	bundle := model.AuditBundle{Manifest: manifest, Entries: []model.AuditEntry{entry}, Signature: signature}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(ctx) }()
	keys := map[string]ed25519.PublicKey{"trusted": public}
	verifier, err := NewVerifier(keys, model.TelemetryOptions{MeterProvider: provider})
	if err != nil {
		t.Fatal(err)
	}
	// Mutating the caller's map and key bytes must not alter established trust.
	public[0] ^= 0xff
	delete(keys, "trusted")
	if err := verifier.Verify(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	tampered := bundle
	tampered.Entries = []model.AuditEntry{{Kind: entry.Kind, ID: entry.ID, Payload: json.RawMessage(`{"value":"tampered"}`)}}
	if err := verifier.Verify(ctx, tampered); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal(err)
	}
	unknown := bundle
	unknown.Signature.KeyID = "PRIVATE-unknown-key"
	if err := verifier.Verify(ctx, unknown); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := verifier.Verify(canceled, tampered); !errors.Is(err, context.Canceled) {
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
				for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
					total += p.Value
					if p.Attributes.Len() != 1 {
						t.Fatal("unexpected integrity labels")
					}
				}
			}
		}
	}
	if total != 2 {
		t.Fatal(total)
	}
	encoded, _ := json.Marshal(data)
	if strings.Contains(string(encoded), "PRIVATE") {
		t.Fatal("sensitive telemetry")
	}
}
