package connectapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"go.opentelemetry.io/otel/baggage"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type telemetryTransport struct{ http.RoundTripper }

func (t telemetryTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	r.Header.Set("tracestate", "private=PRIVATE-header")
	r.Header.Set("baggage", "private=PRIVATE-baggage")
	return t.RoundTripper.RoundTrip(r)
}
func TestRPCTelemetryPropagationPrivacyAndAuthentication(t *testing.T) {
	for _, accept := range []bool{false, true} {
		t.Run(map[bool]string{false: "local parent", true: "remote parent"}[accept], func(t *testing.T) {
			ctx := context.Background()
			exporter := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			defer func() { _ = tp.Shutdown(ctx) }()
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			defer func() { _ = mp.Shutdown(ctx) }()
			var logs bytes.Buffer
			var denied atomic.Bool
			engine := &testEngine{get: func(ctx context.Context, _ model.Caller, _ model.FactQuery) (model.FactResult, error) {
				sc := trace.SpanContextFromContext(ctx)
				if !sc.IsValid() || (sc.TraceID().String() == "4bf92f3577b34da6a3ce929d0e0e4736") != accept || sc.TraceState().String() != "" || baggage.FromContext(ctx).Len() != 0 {
					t.Error("incorrect trace propagation or baggage extraction")
				}
				return model.FactResult{}, errors.New("PRIVATE-evidence-provider-error")
			}}
			verify := func(ctx context.Context) (model.Caller, error) {
				if denied.Load() {
					return model.Caller{}, errors.New("PRIVATE-credential")
				}
				return fixedIdentity(ctx)
			}
			handler, err := NewHandlerWithOptions(engine, verify, testLimits(), HandlerOptions{AcceptTraceContext: accept, Telemetry: model.TelemetryOptions{TracerProvider: tp, MeterProvider: mp, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			httpClient := &http.Client{Transport: telemetryTransport{server.Client().Transport}}
			client := memoryv1connect.NewFactServiceClient(connect.NewClient(connecthttp.NewTransport(httpClient, server.URL)))
			request := validQuery()
			request.Purpose = "PRIVATE-purpose"
			if _, err := client.GetFact(ctx, request); connect.CodeOf(err) != connect.CodeInternal {
				t.Fatal(err)
			}
			denied.Store(true)
			if _, err := client.GetFact(ctx, request); connect.CodeOf(err) != connect.CodeUnauthenticated || engine.calls.Load() != 1 {
				t.Fatal("trace headers bypassed identity", err)
			}
			response, err := httpClient.Get(server.URL + "/PRIVATE-arbitrary-path")
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			spans := exporter.GetSpans()
			if len(spans) != 2 {
				t.Fatal("missing RPC spans or arbitrary route recorded", len(spans))
			}
			if spans[0].Name != "serve.rpc" || spans[0].SpanKind != trace.SpanKindServer || spans[0].Status.Description != "internal" || spans[1].Status.Description != "unauthenticated" {
				t.Fatal("incorrect RPC outcomes", spans)
			}
			if accept && spans[0].Parent.SpanID().String() != "00f067aa0ba902b7" {
				t.Fatal("remote parent lost")
			}
			var metrics metricdata.ResourceMetrics
			if err := reader.Collect(ctx, &metrics); err != nil {
				t.Fatal(err)
			}
			count := int64(0)
			for _, scope := range metrics.ScopeMetrics {
				for _, m := range scope.Metrics {
					if m.Name == "facts.rpc.requests" {
						for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
							count += point.Value
							if point.Attributes.Len() != 2 {
								t.Fatal("unbounded RPC labels")
							}
						}
					}
				}
			}
			if count != 2 {
				t.Fatal("incorrect request count", count)
			}
			encoded, _ := json.Marshal(struct {
				Spans   any
				Metrics any
			}{spans, metrics})
			if strings.Contains(string(encoded)+logs.String(), "PRIVATE") {
				t.Fatal("private RPC content reached telemetry")
			}
		})
	}
}

type selectedTraceTransport struct {
	http.RoundTripper
	parents []string
}

func (t selectedTraceTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header["Traceparent"] = t.parents
	return t.RoundTripper.RoundTrip(r)
}
func TestMalformedAndDuplicateTraceParentsAreIgnored(t *testing.T) {
	const valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	for _, parents := range [][]string{{valid, valid}, {"invalid"}, {valid + strings.Repeat("a", 512)}} {
		ctx := context.Background()
		tp := sdktrace.NewTracerProvider()
		engine := &testEngine{get: func(ctx context.Context, _ model.Caller, _ model.FactQuery) (model.FactResult, error) {
			if trace.SpanContextFromContext(ctx).TraceID().String() == "4bf92f3577b34da6a3ce929d0e0e4736" {
				t.Error("invalid or ambiguous traceparent accepted")
			}
			return model.FactResult{}, model.ErrNotFound
		}}
		handler, err := NewHandlerWithOptions(engine, fixedIdentity, testLimits(), HandlerOptions{AcceptTraceContext: true, Telemetry: model.TelemetryOptions{TracerProvider: tp}})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		client := memoryv1connect.NewFactServiceClient(connect.NewClient(connecthttp.NewTransport(&http.Client{Transport: selectedTraceTransport{server.Client().Transport, parents}}, server.URL)))
		if _, err := client.GetFact(ctx, validQuery()); connect.CodeOf(err) != connect.CodeNotFound {
			t.Fatal("trace metadata changed RPC behavior", err)
		}
		server.Close()
		_ = tp.Shutdown(ctx)
	}
}
