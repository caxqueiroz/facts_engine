// Package telemetry records only fixed operation names and classified outcomes.
// Never pass caller-controlled strings as operation names or attributes.
package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

type Observer struct {
	extractionResults, extractionProposals, extractionIssues, extractionRejected metric.Int64Counter
	tracer                                                                       trace.Tracer
	count                                                                        metric.Int64Counter
	duration                                                                     metric.Float64Histogram
	logger                                                                       *slog.Logger
}

func New(o model.TelemetryOptions) (*Observer, error) {
	if o.TracerProvider == nil && o.MeterProvider == nil && o.Logger == nil {
		return nil, nil
	}
	if o.TracerProvider == nil {
		o.TracerProvider = tracenoop.NewTracerProvider()
	}
	if o.MeterProvider == nil {
		o.MeterProvider = metricnoop.NewMeterProvider()
	}
	meter := o.MeterProvider.Meter("github.com/caxqueiroz/facts_engine")
	count, err := meter.Int64Counter("facts.operations", metric.WithDescription("Completed engine operations by stage and outcome"))
	if err != nil {
		return nil, model.ErrInvalid
	}
	duration, err := meter.Float64Histogram("facts.operation.duration", metric.WithUnit("s"), metric.WithDescription("Engine operation duration"))
	if err != nil {
		return nil, model.ErrInvalid
	}
	results, err := meter.Int64Counter("facts.extraction.results")
	if err != nil {
		return nil, model.ErrInvalid
	}
	proposals, err := meter.Int64Counter("facts.extraction.proposals")
	if err != nil {
		return nil, model.ErrInvalid
	}
	issues, err := meter.Int64Counter("facts.extraction.issues")
	if err != nil {
		return nil, model.ErrInvalid
	}
	rejected, err := meter.Int64Counter("facts.extraction.rejected_jobs")
	if err != nil {
		return nil, model.ErrInvalid
	}
	return &Observer{tracer: o.TracerProvider.Tracer("github.com/caxqueiroz/facts_engine"), count: count, duration: duration, logger: o.Logger, extractionResults: results, extractionProposals: proposals, extractionIssues: issues, extractionRejected: rejected}, nil
}

type Operation struct {
	observer *Observer
	ctx      context.Context
	span     trace.Span
	stage    string
	start    time.Time
}

func (o *Observer) Start(ctx context.Context, stage string) (context.Context, *Operation) {
	if o == nil {
		return ctx, nil
	}
	ctx, span := o.tracer.Start(ctx, stage)
	return ctx, &Operation{o, ctx, span, stage, time.Now()}
}

// End must be deferred directly. It never records error or panic text, and
// rethrows panics after recording a bounded outcome.
func (op *Operation) End(err *error) {
	if op == nil {
		return
	}
	failure := recover()
	outcome := classify(*err)
	if failure != nil {
		outcome = "panic"
	}
	elapsed := time.Since(op.start).Seconds()
	attrs := []attribute.KeyValue{attribute.String("facts.stage", op.stage), attribute.String("facts.outcome", outcome)}
	op.span.SetAttributes(attrs...)
	if outcome != "ok" {
		op.span.SetStatus(codes.Error, outcome)
	}
	op.observer.count.Add(op.ctx, 1, metric.WithAttributes(attrs...))
	op.observer.duration.Record(op.ctx, elapsed, metric.WithAttributes(attrs...))
	if logger := op.observer.logger; logger != nil {
		level := slog.LevelDebug
		if outcome != "ok" {
			level = slog.LevelWarn
		}
		fields := []any{"stage", op.stage, "outcome", outcome, "duration_seconds", elapsed}
		if sc := op.span.SpanContext(); sc.IsValid() {
			fields = append(fields, "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
		}
		logger.Log(op.ctx, level, "engine operation", fields...)
	}
	op.span.End()
	if failure != nil {
		panic(failure)
	}
}
func classify(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, model.ErrDenied):
		return "denied"
	case errors.Is(err, model.ErrInvalid):
		return "invalid"
	case errors.Is(err, model.ErrNotFound):
		return "not_found"
	case errors.Is(err, model.ErrRevision):
		return "revision_conflict"
	case errors.Is(err, model.ErrConflict):
		return "conflict"
	case errors.Is(err, model.ErrPrecondition):
		return "precondition"
	case errors.Is(err, model.ErrQuota):
		return "quota"
	case errors.Is(err, model.ErrUnavailable):
		return "unavailable"
	default:
		return "internal"
	}
}
