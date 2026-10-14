package connectapi

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/caxqueiroz/facts_engine/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// HandlerOptions is opt-in, host-owned observability. Trace headers never grant
// identity or permissions. Only traceparent is accepted; baggage and tracestate
// are not extracted from requests.
type HandlerOptions struct {
	Telemetry          model.TelemetryOptions
	AcceptTraceContext bool
}
type rpcObserver struct {
	tracer   trace.Tracer
	count    metric.Int64Counter
	duration metric.Float64Histogram
	logger   *slog.Logger
}

func newRPCObserver(o model.TelemetryOptions) (*rpcObserver, error) {
	if o.TracerProvider == nil && o.MeterProvider == nil && o.Logger == nil {
		return nil, nil
	}
	if o.TracerProvider == nil {
		o.TracerProvider = tracenoop.NewTracerProvider()
	}
	if o.MeterProvider == nil {
		o.MeterProvider = metricnoop.NewMeterProvider()
	}
	meter := o.MeterProvider.Meter("github.com/caxqueiroz/facts_engine/connectapi")
	count, err := meter.Int64Counter("facts.rpc.requests")
	if err != nil {
		return nil, model.ErrInvalid
	}
	duration, err := meter.Float64Histogram("facts.rpc.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, model.ErrInvalid
	}
	return &rpcObserver{o.TracerProvider.Tracer("github.com/caxqueiroz/facts_engine/connectapi"), count, duration, o.Logger}, nil
}

type rpcObservation struct {
	observer  *rpcObserver
	ctx       context.Context
	span      trace.Span
	procedure string
	start     time.Time
}

func (o *rpcObserver) start(ctx context.Context, procedure string) (context.Context, *rpcObservation) {
	if o == nil {
		return ctx, nil
	}
	ctx, span := o.tracer.Start(ctx, "serve.rpc", trace.WithSpanKind(trace.SpanKindServer))
	return ctx, &rpcObservation{o, ctx, span, procedure, time.Now()}
}
func (o *rpcObservation) end(err *error) {
	if o == nil {
		return
	}
	failure := recover()
	outcome := "ok"
	if *err != nil {
		outcome = connect.CodeOf(*err).String()
	}
	if failure != nil {
		outcome = "panic"
	}
	attrs := []attribute.KeyValue{attribute.String("rpc.method", o.procedure), attribute.String("rpc.outcome", outcome)}
	o.span.SetAttributes(attrs...)
	if outcome != "ok" {
		o.span.SetStatus(codes.Error, outcome)
	}
	seconds := time.Since(o.start).Seconds()
	o.observer.count.Add(o.ctx, 1, metric.WithAttributes(attrs...))
	o.observer.duration.Record(o.ctx, seconds, metric.WithAttributes(attrs...))
	if logger := o.observer.logger; logger != nil {
		fields := []any{"method", o.procedure, "outcome", outcome, "duration_seconds", seconds}
		if sc := o.span.SpanContext(); sc.IsValid() {
			fields = append(fields, "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
		}
		level := slog.LevelDebug
		if outcome != "ok" {
			level = slog.LevelWarn
		}
		logger.Log(o.ctx, level, "RPC completed", fields...)
	}
	o.span.End()
	if failure != nil {
		panic(failure)
	}
}
