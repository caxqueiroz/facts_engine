package extractionhttp

import (
	"context"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOptionalTracePropagation(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if (r.Header.Get("Traceparent") != "") != enabled || r.Header.Get("Authorization") != "Bearer host-token" || r.Header.Get("Baggage") != "" || r.Header.Get("Tracestate") != "" {
				t.Error("unsafe gateway propagation")
			}
			metadata(w)
			_, _ = w.Write([]byte(`{}`))
		}))
		provider, err := New(Config{URL: server.URL, Token: "host-token", AllowPlaintextLoopback: true, Timeout: time.Second, MaxInputBytes: 100, MaxOutputBytes: 100, PropagateTraceContext: enabled})
		if err != nil {
			t.Fatal(err)
		}
		ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}}))
		result, err := provider.Extract(ctx, []byte(`{}`))
		if err != nil || string(result.Body) != "{}" {
			t.Fatal("propagation changed extraction", err)
		}
		provider.Close()
		server.Close()
	}
}
