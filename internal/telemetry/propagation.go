package telemetry

import (
	"context"
	"go.opentelemetry.io/otel/propagation"
	"net/http"
)

// InjectTraceParent copies only the W3C traceparent from the active context.
// It does not use global propagators or forward baggage/tracestate.
func InjectTraceParent(ctx context.Context, header http.Header) {
	carrier := propagation.MapCarrier{}
	(propagation.TraceContext{}).Inject(ctx, carrier)
	if parent := carrier.Get("traceparent"); parent != "" {
		header.Set("Traceparent", parent)
	}
}
