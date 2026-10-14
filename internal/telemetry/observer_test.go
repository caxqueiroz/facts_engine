package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestInjectedTelemetryPrivacyAndParentage(t *testing.T) {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(ctx) }()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()
	var logs bytes.Buffer
	observer, err := New(model.TelemetryOptions{TracerProvider: tp, MeterProvider: mp, Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err != nil {
		t.Fatal(err)
	}
	ctx, parent := tp.Tracer("host").Start(ctx, "host.request")
	var child trace.SpanContext
	func() (err error) {
		ctx, op := observer.Start(ctx, "check.authorization")
		defer op.End(&err)
		child = trace.SpanContextFromContext(ctx)
		return errors.New("PRIVATE-provider-response-and-credential")
	}()
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "check.authorization" || spans[0].Parent.SpanID() != parent.SpanContext().SpanID() || child.TraceID() != parent.SpanContext().TraceID() || spans[0].Status.Description != "internal" {
		t.Fatal("lost injected trace or outcome", spans)
	}
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &metrics); err != nil {
		t.Fatal(err)
	}
	count := int64(0)
	for _, scope := range metrics.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "facts.operations" {
				for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
					count += point.Value
					if point.Attributes.Len() != 2 {
						t.Fatal("unexpected metric labels", point.Attributes)
					}
				}
			}
		}
	}
	if count != 1 {
		t.Fatal("missing operation count", count)
	}
	encoded, _ := json.Marshal(struct {
		Spans   any
		Metrics any
	}{spans, metrics})
	if strings.Contains(string(encoded)+logs.String(), "PRIVATE") || !strings.Contains(logs.String(), child.TraceID().String()) {
		t.Fatal("sensitive content leaked or log trace missing", logs.String())
	}
	parent.End()
}
func TestNoopAndPanicBehavior(t *testing.T) {
	observer, err := New(model.TelemetryOptions{})
	if err != nil || observer != nil {
		t.Fatal("default telemetry enabled")
	}
	ctx := context.Background()
	same, op := observer.Start(ctx, "run.extraction")
	if same != ctx || op != nil {
		t.Fatal("noop changed context")
	}
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(ctx) }()
	observer, err = New(model.TelemetryOptions{TracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() != "PRIVATE-panic" {
				t.Error("panic changed")
			}
		}()
		var err error
		_, op := observer.Start(ctx, "run.extraction")
		defer op.End(&err)
		panic("PRIVATE-panic")
	}()
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Description != "panic" || len(spans[0].Events) != 0 {
		t.Fatal("panic leaked or suppressed", spans)
	}
}
