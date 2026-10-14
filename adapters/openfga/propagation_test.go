package openfga

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOptionalTracePropagation(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if (r.Header.Get("Traceparent") != "") != enabled || r.Header.Get("Authorization") != "Bearer host-token" || r.Header.Get("Baggage") != "" || r.Header.Get("Tracestate") != "" {
				t.Error("trace changed authorization headers")
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"allowed":true}`))
		}))
		auth, err := New(Config{URL: server.URL, StoreID: "store", ModelID: "model", Token: "host-token", PropagateTraceContext: enabled})
		if err != nil {
			t.Fatal(err)
		}
		ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}}))
		decision, err := auth.Check(ctx, model.AuthorizationRequest{Caller: model.Caller{Tenant: "tenant", Principal: "worker"}, Scope: "scope", Permission: "can_read", Purpose: "review"})
		if err != nil || !decision.Allowed {
			t.Fatal("propagation changed check", err)
		}
		server.Close()
	}
}
