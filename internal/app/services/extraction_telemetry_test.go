package services

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/caxqueiroz/facts_engine/internal/telemetry"
	"github.com/caxqueiroz/facts_engine/model"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type observedExtractionFunc func(context.Context, []byte) (model.ExtractionResponse, error)

func (f observedExtractionFunc) Extract(ctx context.Context, b []byte) (model.ExtractionResponse, error) {
	return f(ctx, b)
}

func TestProviderStagePropagationOutcomesAndPrivacy(t *testing.T) {
	for _, outcome := range []string{"ok", "internal", "canceled", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			exporter := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			defer func() { _ = tp.Shutdown(ctx) }()
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() { _ = mp.Shutdown(ctx) }()
			var logs bytes.Buffer
			observer, err := telemetry.New(model.TelemetryOptions{TracerProvider: tp, MeterProvider: mp, Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
			if err != nil {
				t.Fatal(err)
			}
			s := &Service{telemetry: observer}
			parent, parentSpan := tp.Tracer("test").Start(ctx, "run.extraction")
			defer parentSpan.End()
			providerCtx, cancel := context.WithCancel(parent)
			defer cancel()
			private := "PRIVATE-prompt-response-error"
			request := []byte(private)
			var seen trace.SpanContext
			provider := observedExtractionFunc(func(ctx context.Context, raw []byte) (model.ExtractionResponse, error) {
				seen = trace.SpanContextFromContext(ctx)
				if string(raw) != private {
					t.Fatal("provider request changed")
				}
				raw[0] = 'X'
				switch outcome {
				case "internal":
					return model.ExtractionResponse{}, errors.New(private)
				case "canceled":
					cancel()
				case "panic":
					panic(private)
				}
				return model.ExtractionResponse{Body: []byte(private)}, nil
			})
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_, err = s.callExtractionProvider(providerCtx, provider, request)
			}()
			if outcome == "panic" && recovered != private {
				t.Fatal("panic was swallowed")
			}
			if outcome != "panic" && recovered != nil {
				t.Fatal(recovered)
			}
			if outcome == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("lost cancellation", err)
			}
			if outcome == "internal" && (err == nil || err.Error() != private) {
				t.Fatal("provider error changed", err)
			}
			if string(request) != private {
				t.Fatal("retained request mutated")
			}
			spans := exporter.GetSpans()
			if len(spans) != 1 || spans[0].Name != "call.extraction_provider" || spans[0].SpanContext.SpanID() != seen.SpanID() || spans[0].Parent.SpanID() != parentSpan.SpanContext().SpanID() {
				t.Fatal("provider span ancestry", spans)
			}
			if len(spans[0].Attributes) != 2 || spans[0].Attributes[1].Value.AsString() != outcome {
				t.Fatal("unexpected outcome", spans[0].Attributes)
			}
			var metrics metricdata.ResourceMetrics
			if err := reader.Collect(ctx, &metrics); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(fmt.Sprint(spans, metrics, logs.String()), private) {
				t.Fatal("private content entered telemetry")
			}
		})
	}
}
