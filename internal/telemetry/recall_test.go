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

func TestRecallFilterMetricBounds(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(ctx) }()
	observer, err := New(model.TelemetryOptions{MeterProvider: provider})
	if err != nil {
		t.Fatal(err)
	}
	observer.RecallFiltered(ctx, "fact", 1, 2)
	observer.RecallFiltered(ctx, "memory", 0, 3)
	observer.RecallFiltered(ctx, "PRIVATE-subject", -1, 1)
	var data metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &data); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "facts.recall.filtered" {
				for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
					total += point.Value
					if point.Attributes.Len() != 2 {
						t.Fatal("unexpected recall labels")
					}
				}
			}
		}
	}
	if total != 7 {
		t.Fatal(total)
	}
	raw, _ := json.Marshal(data)
	if strings.Contains(string(raw), "PRIVATE") {
		t.Fatal("unbounded recall label")
	}
}
