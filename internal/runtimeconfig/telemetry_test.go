package runtimeconfig

import (
	"context"
	"encoding/pem"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func telemetryConfig(endpoint string) *Telemetry {
	return &Telemetry{Endpoint: endpoint, ExportTimeoutSeconds: 1, MetricIntervalSeconds: 300, TraceSampleRatio: 1}
}
func TestTelemetryValidation(t *testing.T) {
	for _, endpoint := range []string{"", "http://localhost", "http://127.0.0.1", "https://user:secret@example.com", "https://example.com?secret=value", "https://example.com/#fragment"} {
		if telemetryConfig(endpoint).Validate() == nil {
			t.Errorf("accepted %q", endpoint)
		}
	}
	c := telemetryConfig("http://127.0.0.1")
	c.AllowPlaintextLoopback = true
	if c.Validate() != nil {
		t.Fatal("explicit loopback rejected")
	}
	c.TraceSampleRatio = math.NaN()
	if c.Validate() == nil {
		t.Fatal("accepted NaN")
	}
	r, err := StartTelemetry(context.Background(), nil, "", nil)
	if err != nil || r.Options.TracerProvider != nil || r.Options.MeterProvider != nil {
		t.Fatal("disabled telemetry allocated providers")
	}
}
func TestTelemetryTLSExport(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("missing explicit credential")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if strings.Contains(string(body), "private-token") || strings.Contains(string(body), "ambient-private") {
			t.Error("private ambient data exported")
		}
		switch r.URL.Path {
		case "/collector/v1/traces":
			var request tracepb.ExportTraceServiceRequest
			if proto.Unmarshal(body, &request) != nil || len(request.ResourceSpans) != 1 {
				t.Error("invalid trace export")
			}
		case "/collector/v1/metrics":
			var request metricpb.ExportMetricsServiceRequest
			if proto.Unmarshal(body, &request) != nil || len(request.ResourceMetrics) != 1 {
				t.Error("invalid metric export")
			}
		default:
			t.Error("unexpected export path")
		}
		mu.Lock()
		seen[r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer server.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_TELEMETRY_TOKEN", "private-token")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=ambient-private")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "private=ambient-private")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	c := telemetryConfig(server.URL + "/collector")
	c.RootCAFile = ca
	c.TokenEnv = "TEST_TELEMETRY_TOKEN"
	ctx := context.Background()
	runtime, err := StartTelemetry(ctx, c, "memoryd", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	_, span := runtime.Options.TracerProvider.Tracer("test").Start(ctx, "test")
	span.End()
	counter, err := runtime.Options.MeterProvider.Meter("test").Int64Counter("test.count")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(ctx, 1)
	if err := runtime.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if seen["/collector/v1/traces"] != 1 || seen["/collector/v1/metrics"] != 1 {
		t.Fatalf("exports: %v", seen)
	}
}
func TestTelemetryCollectorErrorsSanitized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "private collector response", 500) }))
	defer server.Close()
	c := telemetryConfig(server.URL)
	c.AllowPlaintextLoopback = true
	runtime, err := StartTelemetry(context.Background(), c, "memoryworker", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	counter, err := runtime.Metrics.Meter("test").Int64Counter("test.count")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(context.Background(), 1)
	err = runtime.Metrics.ForceFlush(context.Background())
	if err == nil || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), server.URL) {
		t.Fatalf("unsafe exporter error: %v", err)
	}
	_ = runtime.Shutdown(context.Background())
}

func TestTelemetryRejectsRedirect(t *testing.T) {
	var mu sync.Mutex
	redirected := false
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		redirected = true
		mu.Unlock()
	}))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	c := telemetryConfig(server.URL)
	c.AllowPlaintextLoopback = true
	runtime, err := StartTelemetry(context.Background(), c, "memoryworker", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	counter, err := runtime.Metrics.Meter("test").Int64Counter("test.count")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(context.Background(), 1)
	if runtime.Metrics.ForceFlush(context.Background()) == nil {
		t.Fatal("redirect accepted")
	}
	_ = runtime.Shutdown(context.Background())
	mu.Lock()
	defer mu.Unlock()
	if redirected {
		t.Fatal("followed collector redirect")
	}
}
