package telemetry

import (
	"context"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"testing"
)

func TestTraceParentOnlyPropagation(t *testing.T) {
	state, _ := trace.ParseTraceState("private=PRIVATE-state")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled, TraceState: state})
	member, _ := baggage.NewMember("private", "PRIVATE-baggage")
	bag, _ := baggage.New(member)
	ctx := baggage.ContextWithBaggage(trace.ContextWithSpanContext(context.Background(), sc), bag)
	header := http.Header{"Authorization": []string{"Bearer host-secret"}}
	InjectTraceParent(ctx, header)
	if header.Get("Traceparent") != "00-01000000000000000000000000000000-0200000000000000-01" || header.Get("Tracestate") != "" || header.Get("Baggage") != "" || header.Get("Authorization") != "Bearer host-secret" {
		t.Fatal("unsafe propagation", header)
	}
	empty := http.Header{}
	InjectTraceParent(context.Background(), empty)
	if len(empty) != 0 {
		t.Fatal("invented trace parent")
	}
}
