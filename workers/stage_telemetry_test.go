package workers

import (
	"bytes"
	"context"
	"crypto/ed25519"
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

type stageProjectionSource struct {
	model.ProjectionSource
	check func(context.Context)
}

func (s stageProjectionSource) ProjectionDocument(ctx context.Context, _ model.Delivery, _ string) (*model.ProjectionDocument, error) {
	s.check(ctx)
	return nil, nil
}

type stageArchiveSource struct {
	model.ArchiveSource
	check func(context.Context)
	err   error
}

func (s stageArchiveSource) ArchivePayload(ctx context.Context, _ model.Delivery) (model.ArchivePayload, error) {
	s.check(ctx)
	return model.ArchivePayload{}, s.err
}

func TestWorkerStageTelemetry(t *testing.T) {
	for _, kind := range []string{"projection", "archive"} {
		for _, outcome := range []string{"ok", "denied", "internal", "canceled", "panic"} {
			if kind == "archive" && outcome == "ok" {
				continue
			} // Full successful archive writes are covered by integration tests.
			t.Run(kind+"/"+outcome, func(t *testing.T) {
				exporter := tracetest.NewInMemoryExporter()
				tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
				defer func() { _ = tp.Shutdown(context.Background()) }()
				reader := sdkmetric.NewManualReader()
				mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
				defer func() { _ = mp.Shutdown(context.Background()) }()
				var logs bytes.Buffer
				options := model.TelemetryOptions{TracerProvider: tp, MeterProvider: mp, Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}
				ctx, parent := tp.Tracer("test").Start(context.Background(), "parent")
				defer parent.End()
				caller := model.Caller{Tenant: "PRIVATE-tenant", Principal: "PRIVATE-principal"}
				var wantErr error
				if outcome == "internal" {
					wantErr = errors.New("PRIVATE-error")
				}
				if outcome == "canceled" {
					wantErr = context.Canceled
				}
				calls := 0
				check := func(ctx context.Context) {
					calls++
					sc := trace.SpanContextFromContext(ctx)
					if sc.TraceID() != parent.SpanContext().TraceID() || sc.SpanID() == parent.SpanContext().SpanID() {
						t.Error("stage context not propagated")
					}
					if outcome == "panic" {
						panic("PRIVATE-panic")
					}
				}
				var handler interface {
					Handle(context.Context, model.Delivery) error
				}
				var err error
				delivery := model.Delivery{Key: model.ConsumerKey{Tenant: caller.Tenant}, Event: model.Event{Type: "accept_fact", LedgerRevision: 1, Scope: "PRIVATE-scope"}}
				stage := "project.document"
				if kind == "projection" {
					source := projectionErrorSource{stageProjectionSource: stageProjectionSource{check: check}, err: wantErr}
					handler, err = NewProjectionWithTelemetry(source, stageIndex{}, &auditAuthorizer{}, caller, "PRIVATE-generation", options)
					delivery.Key.Consumer = model.ProjectionConsumer("PRIVATE-generation")
				} else {
					stage = "archive.event"
					handler, err = NewArchiveWithTelemetry(stageArchiveSource{check: check, err: wantErr}, forbiddenArchiveStore{}, forbiddenArchiveSigner{}, map[string]ed25519.PublicKey{"PRIVATE-key": make([]byte, ed25519.PublicKeySize)}, &auditAuthorizer{}, caller, "PRIVATE-stream", options)
					delivery.Key.Consumer = model.ArchiveConsumer("PRIVATE-stream")
				}
				if err != nil {
					t.Fatal(err)
				}
				if outcome == "denied" {
					delivery.Key.Tenant = "wrong"
					wantErr = model.ErrDenied
				}
				func() {
					defer func() {
						p := recover()
						if outcome == "panic" {
							if p != "PRIVATE-panic" {
								t.Errorf("panic changed: %v", p)
							}
						} else if p != nil {
							t.Errorf("unexpected panic: %v", p)
						}
					}()
					err = handler.Handle(ctx, delivery)
					if outcome == "panic" {
						t.Error("panic swallowed")
					}
					if !errors.Is(err, wantErr) {
						t.Errorf("error changed: %v", err)
					}
				}()
				if outcome == "denied" && calls != 0 {
					t.Fatal("unauthorized source access")
				}
				spans := exporter.GetSpans()
				if len(spans) != 1 || spans[0].Name != stage || spans[0].Parent.SpanID() != parent.SpanContext().SpanID() {
					t.Fatal("missing stage parentage", spans)
				}
				got := ""
				for _, a := range spans[0].Attributes {
					if string(a.Key) == "facts.outcome" {
						got = a.Value.AsString()
					}
				}
				if got != outcome {
					t.Fatalf("outcome %q, want %q", got, outcome)
				}
				var data metricdata.ResourceMetrics
				if err := reader.Collect(ctx, &data); err != nil {
					t.Fatal(err)
				}
				count := int64(0)
				for _, scope := range data.ScopeMetrics {
					for _, m := range scope.Metrics {
						if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
							for _, point := range sum.DataPoints {
								count += point.Value
							}
						}
					}
				}
				if count != 1 {
					t.Fatalf("operation count %d", count)
				}
				if strings.Contains(logs.String(), "PRIVATE") {
					t.Fatal("private data logged")
				}
			})
		}
	}
}

type stageIndex struct{ model.RetrievalIndex }
type projectionErrorSource struct {
	stageProjectionSource
	err error
}

func (s projectionErrorSource) ProjectionDocument(ctx context.Context, d model.Delivery, g string) (*model.ProjectionDocument, error) {
	_, _ = s.stageProjectionSource.ProjectionDocument(ctx, d, g)
	return nil, s.err
}
