package workers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type observedQueue struct {
	queueFake
	claimErr, ackErr, failErr error
}

func (q *observedQueue) ClaimDelivery(ctx context.Context, _ model.ConsumerKey, _ time.Duration) (model.Delivery, error) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return model.Delivery{}, errors.New("missing claim trace")
	}
	return model.Delivery{Attempt: 2, Key: model.ConsumerKey{Tenant: "PRIVATE-tenant", Consumer: "PRIVATE-consumer"}, Event: model.Event{ID: "PRIVATE-event", Scope: "PRIVATE-scope", RecordedAt: time.Now().Add(-time.Minute), Command: model.Command{Purpose: "PRIVATE-purpose"}}}, q.claimErr
}
func (q *observedQueue) AcknowledgeDelivery(ctx context.Context, d model.Delivery) error {
	_ = q.queueFake.AcknowledgeDelivery(ctx, d)
	return q.ackErr
}
func (q *observedQueue) FailDelivery(ctx context.Context, d model.Delivery, delay time.Duration) error {
	_ = q.queueFake.FailDelivery(ctx, d, delay)
	return q.failErr
}
func TestOutboxTelemetryOutcomesPrivacyAndContext(t *testing.T) {
	for _, outcome := range []string{"idle", "claim_error", "acknowledged", "ack_error", "failure_recorded", "failure_record_error", "canceled", "lease_error"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			exporter := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			defer func() { _ = tp.Shutdown(context.Background()) }()
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() { _ = mp.Shutdown(context.Background()) }()
			var logs bytes.Buffer
			q := &observedQueue{}
			private := errors.New("PRIVATE-dependency-error")
			switch outcome {
			case "idle":
				q.claimErr = model.ErrNotFound
			case "claim_error":
				q.claimErr = private
			case "ack_error":
				q.ackErr = private
			case "failure_record_error":
				q.failErr = private
			case "lease_error":
				q.renewError = private
			}
			handler := HandlerFunc(func(ctx context.Context, _ model.Delivery) error {
				if !trace.SpanContextFromContext(ctx).IsValid() {
					t.Error("handler context lost trace")
				}
				switch outcome {
				case "failure_recorded", "failure_record_error":
					return private
				case "canceled":
					cancel()
					return ctx.Err()
				case "lease_error":
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			})
			w, err := NewOutbox(q, model.ConsumerKey{Tenant: "PRIVATE-tenant", Consumer: "PRIVATE-consumer"}, handler, Options{Lease: 3 * time.Second, PollInterval: time.Millisecond, MinBackoff: time.Millisecond, MaxBackoff: time.Second, Telemetry: model.TelemetryOptions{TracerProvider: tp, MeterProvider: mp, Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}})
			if err != nil {
				t.Fatal(err)
			}
			if outcome == "lease_error" {
				w.options.Lease = 30 * time.Millisecond
			}
			_, _ = w.ProcessOne(ctx)
			spans := exporter.GetSpans()
			if len(spans) != 1 || spans[0].Name != "process.outbox" || spans[0].SpanKind != trace.SpanKindConsumer {
				t.Fatal("missing worker span", spans)
			}
			var metrics metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &metrics); err != nil {
				t.Fatal(err)
			}
			polls, retries, ages := int64(0), int64(0), uint64(0)
			for _, scope := range metrics.ScopeMetrics {
				for _, m := range scope.Metrics {
					switch m.Name {
					case "facts.outbox.polls":
						for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
							polls += point.Value
							value, _ := point.Attributes.Value("worker.outcome")
							if value.AsString() != outcome || point.Attributes.Len() != 2 {
								t.Fatal("incorrect outcome labels", point.Attributes)
							}
						}
					case "facts.outbox.retries":
						for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
							retries += point.Value
							if point.Attributes.Len() != 1 {
								t.Fatal("retry identity labels")
							}
						}
					case "facts.outbox.event_age":
						for _, point := range m.Data.(metricdata.Histogram[float64]).DataPoints {
							ages += point.Count
							if point.Sum < 30 {
								t.Fatal("missing event age", point.Sum)
							}
						}
					}
				}
			}
			if polls != 1 {
				t.Fatal("poll count", polls)
			}
			claimed := outcome != "idle" && outcome != "claim_error"
			if (retries == 1) != claimed || (ages == 1) != claimed {
				t.Fatal("claim measurements incorrect", retries, ages)
			}
			encoded, _ := json.Marshal(struct {
				Spans   any
				Metrics any
			}{spans, metrics})
			if strings.Contains(string(encoded)+logs.String(), "PRIVATE") {
				t.Fatal("worker identifiers or error leaked")
			}
			if outcome == "failure_recorded" && (q.acks != 0 || q.failures != 1) {
				t.Fatal("recorded failure acknowledged")
			}
		})
	}
}
