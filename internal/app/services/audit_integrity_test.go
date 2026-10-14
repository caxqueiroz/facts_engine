package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/internal/telemetry"
	"github.com/caxqueiroz/facts_engine/model"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type auditIntegrityStore struct {
	model.EvidenceStore
	body []byte
	err  error
}

func (s auditIntegrityStore) Read(context.Context, string, model.ObjectRef) ([]byte, error) {
	return s.body, s.err
}

func TestAuditEvidenceIntegrityMetricsDistinguishUnavailable(t *testing.T) {
	for _, test := range []struct {
		name  string
		body  []byte
		err   error
		count int64
		gap   bool
	}{
		{"valid", []byte("ok"), nil, 0, false},
		{"digest", []byte("no"), nil, 1, true},
		{"size", []byte("long"), nil, 1, true},
		{"unavailable", nil, errors.New("PRIVATE-storage-error"), 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() { _ = provider.Shutdown(ctx) }()
			observer, err := telemetry.New(model.TelemetryOptions{MeterProvider: provider})
			if err != nil {
				t.Fatal(err)
			}
			service := &Service{evidence: auditIntegrityStore{body: test.body, err: test.err}, telemetry: observer}
			evidence := model.Evidence{ID: "PRIVATE-evidence", Object: model.ObjectRef{Size: 2, Digest: model.BytesDigest([]byte("ok"))}}
			payload, _ := json.Marshal(evidence)
			_, gaps, err := service.prepareAuditEntries(ctx, model.Caller{Tenant: "PRIVATE-tenant"}, model.AuditExportRequest{IncludeEvidence: true}, []model.AuditEntry{{Kind: "evidence", ID: evidence.ID, Payload: payload}})
			if err != nil || (len(gaps) > 0) != test.gap {
				t.Fatal(gaps, err)
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
						}
					}
				}
			}
			if total != test.count {
				t.Fatal(total, test.count)
			}
		})
	}
}
