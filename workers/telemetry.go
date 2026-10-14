package workers

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"log/slog"
	"time"
)

type outboxTelemetry struct {
	tracer         trace.Tracer
	polls, retries metric.Int64Counter
	duration, age  metric.Float64Histogram
	logger         *slog.Logger
	kind           string
}

func newOutboxTelemetry(o model.TelemetryOptions, h Handler) (*outboxTelemetry, error) {
	if o.TracerProvider == nil && o.MeterProvider == nil && o.Logger == nil {
		return nil, nil
	}
	if o.TracerProvider == nil {
		o.TracerProvider = tracenoop.NewTracerProvider()
	}
	if o.MeterProvider == nil {
		o.MeterProvider = metricnoop.NewMeterProvider()
	}
	kind := "custom"
	switch h.(type) {
	case *Projection:
		kind = "projection"
	case *Archive:
		kind = "archive"
	case *AuditExport:
		kind = "audit"
	case *Extraction:
		kind = "extraction"
	case *MemoryRefresh:
		kind = "refresh"
	}
	m := o.MeterProvider.Meter("github.com/caxqueiroz/facts_engine/workers")
	polls, err := m.Int64Counter("facts.outbox.polls")
	if err != nil {
		return nil, model.ErrInvalid
	}
	retries, err := m.Int64Counter("facts.outbox.retries")
	if err != nil {
		return nil, model.ErrInvalid
	}
	duration, err := m.Float64Histogram("facts.outbox.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, model.ErrInvalid
	}
	age, err := m.Float64Histogram("facts.outbox.event_age", metric.WithUnit("s"))
	if err != nil {
		return nil, model.ErrInvalid
	}
	return &outboxTelemetry{o.TracerProvider.Tracer("github.com/caxqueiroz/facts_engine/workers"), polls, retries, duration, age, o.Logger, kind}, nil
}

type outboxObservation struct {
	telemetry *outboxTelemetry
	ctx       context.Context
	span      trace.Span
	start     time.Time
}

func (o *outboxTelemetry) start(ctx context.Context) (context.Context, *outboxObservation) {
	if o == nil {
		return ctx, nil
	}
	ctx, span := o.tracer.Start(ctx, "process.outbox", trace.WithSpanKind(trace.SpanKindConsumer))
	return ctx, &outboxObservation{o, ctx, span, time.Now()}
}
func (o *outboxObservation) claimed(d model.Delivery) {
	if o == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("worker.kind", o.telemetry.kind))
	if d.Attempt > 1 {
		o.telemetry.retries.Add(o.ctx, 1, attrs)
	}
	if !d.Event.RecordedAt.IsZero() {
		o.telemetry.age.Record(o.ctx, max(0, time.Since(d.Event.RecordedAt).Seconds()), attrs)
	}
}
func (o *outboxObservation) end(outcome *string) {
	if o == nil {
		return
	}
	failure := recover()
	if failure != nil {
		*outcome = "panic"
	}
	attrs := []attribute.KeyValue{attribute.String("worker.kind", o.telemetry.kind), attribute.String("worker.outcome", *outcome)}
	o.span.SetAttributes(attrs...)
	success := *outcome == "acknowledged" || *outcome == "idle"
	if !success {
		o.span.SetStatus(codes.Error, *outcome)
	}
	seconds := time.Since(o.start).Seconds()
	o.telemetry.polls.Add(o.ctx, 1, metric.WithAttributes(attrs...))
	o.telemetry.duration.Record(o.ctx, seconds, metric.WithAttributes(attrs...))
	if logger := o.telemetry.logger; logger != nil {
		fields := []any{"kind", o.telemetry.kind, "outcome", *outcome, "duration_seconds", seconds}
		if sc := o.span.SpanContext(); sc.IsValid() {
			fields = append(fields, "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
		}
		level := slog.LevelDebug
		if !success {
			level = slog.LevelWarn
		}
		logger.Log(o.ctx, level, "outbox poll completed", fields...)
	}
	o.span.End()
	if failure != nil {
		panic(failure)
	}
}
