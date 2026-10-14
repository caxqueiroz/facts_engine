package telemetry

import (
	"context"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/model"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"strings"
	"testing"
)

func TestExtractionBusinessMetricBounds(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(ctx) }()
	observer, err := New(model.TelemetryOptions{MeterProvider: provider})
	if err != nil {
		t.Fatal(err)
	}
	observer.ExtractionResult(ctx, "extraction", "provider_unavailable", 0, 0)
	observer.ExtractionResult(ctx, "memory_refresh", "completed", 1, 3)
	observer.ExtractionResult(ctx, "PRIVATE-provider-id", "PRIVATE-error-text", -1, -1)
	observer.ExtractionRejected(ctx, "PRIVATE-profile-id")
	var data metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &data); err != nil {
		t.Fatal(err)
	}
	totals := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, point := range sum.DataPoints {
					totals[m.Name] += point.Value
					if point.Attributes.Len() != 2 {
						t.Fatal("unexpected business labels")
					}
				}
			}
		}
	}
	if totals["facts.extraction.results"] != 3 || totals["facts.extraction.proposals"] != 1 || totals["facts.extraction.issues"] != 3 || totals["facts.extraction.rejected_jobs"] != 1 {
		t.Fatal(totals)
	}
	raw, _ := json.Marshal(data)
	if strings.Contains(string(raw), "PRIVATE") {
		t.Fatal("unbounded metric label")
	}
}
