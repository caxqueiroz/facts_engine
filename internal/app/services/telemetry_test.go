package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"testing"
)

type observedAuth struct{ seen trace.SpanContext }

func (a *observedAuth) Check(ctx context.Context, _ model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	a.seen = trace.SpanContextFromContext(ctx)
	return model.AuthorizationDecision{}, nil
}

type observedLedger struct {
	model.Ledger
	seen trace.SpanContext
}

func (l *observedLedger) Transact(ctx context.Context, _ string, fn func(model.Tx) error) error {
	l.seen = trace.SpanContextFromContext(ctx)
	return fn(nil)
}

type observedEvidence struct{ model.EvidenceStore }

func TestEngineInjectsTelemetryIntoAuthorizationAndCommit(t *testing.T) {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(ctx) }()
	ledger := &observedLedger{}
	auth := &observedAuth{}
	s, err := NewWithOptions(ledger, auth, observedEvidence{}, model.EngineOptions{Telemetry: model.TelemetryOptions{TracerProvider: tp}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.authorize(ctx, model.Caller{Tenant: "tenant", Principal: "private-principal"}, "private-scope", "can_read", model.Command{Purpose: "private-purpose"}); !errors.Is(err, model.ErrDenied) || !auth.seen.IsValid() {
		t.Fatal("authorization not instrumented", err)
	}
	if err := s.commitTransaction(ctx, "tenant", func(ctx context.Context, _ model.Tx) error {
		if trace.SpanContextFromContext(ctx).SpanID() != ledger.seen.SpanID() {
			t.Fatal("transaction callback lost span")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 || spans[0].Name != "check.authorization" || spans[1].Name != "commit.transaction" {
		t.Fatal(spans)
	}
	for _, span := range spans {
		if len(span.Attributes) != 2 {
			t.Fatal("unexpected identifiers in span", span.Attributes)
		}
	}
}
