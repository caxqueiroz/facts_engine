package runtimeconfig

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Telemetry configures an explicitly started runtime's OTLP/HTTP collector.
// Credentials come from the named environment variable, never JSON config.
type Telemetry struct {
	Endpoint               string  `json:"endpoint"`
	RootCAFile             string  `json:"root_ca_file,omitempty"`
	TokenEnv               string  `json:"token_env,omitempty"`
	AllowPlaintextLoopback bool    `json:"allow_plaintext_loopback,omitempty"`
	ExportTimeoutSeconds   int     `json:"export_timeout_seconds"`
	MetricIntervalSeconds  int     `json:"metric_interval_seconds"`
	TraceSampleRatio       float64 `json:"trace_sample_ratio"`
}

func (c *Telemetry) Validate() error {
	if c == nil {
		return nil
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || len(c.Endpoint) > 2048 || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return model.ErrInvalid
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || !c.AllowPlaintextLoopback || ip == nil || !ip.IsLoopback() {
			return model.ErrInvalid
		}
	}
	if c.ExportTimeoutSeconds < 1 || c.ExportTimeoutSeconds > 30 || c.MetricIntervalSeconds < 1 || c.MetricIntervalSeconds > 300 || math.IsNaN(c.TraceSampleRatio) || math.IsInf(c.TraceSampleRatio, 0) || c.TraceSampleRatio < 0 || c.TraceSampleRatio > 1 || (c.TokenEnv != "" && !envName.MatchString(c.TokenEnv)) {
		return model.ErrInvalid
	}
	return nil
}

type RuntimeTelemetry struct {
	Options   model.TelemetryOptions
	Metrics   *sdkmetric.MeterProvider
	traces    *sdktrace.TracerProvider
	transport *http.Transport
}

// StartTelemetry is used only by explicit runtime composition, not engine
// constructors. Providers, SDK workers and the transport are owned by the result.
func StartTelemetry(ctx context.Context, c *Telemetry, service string, logger *slog.Logger) (*RuntimeTelemetry, error) {
	runtime := &RuntimeTelemetry{}
	if c == nil {
		return runtime, nil
	}
	if c.Validate() != nil || logger == nil || !model.ValidID(service) {
		return nil, model.ErrInvalid
	}
	var roots *x509.CertPool
	if c.RootCAFile != "" {
		file, err := os.Open(c.RootCAFile)
		if err != nil {
			return nil, errors.New("cannot open telemetry CA")
		}
		raw, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		_ = file.Close()
		if err != nil || len(raw) > 1<<20 {
			return nil, errors.New("invalid telemetry CA")
		}
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(raw) {
			return nil, errors.New("invalid telemetry CA")
		}
	}
	headers := map[string]string{}
	if c.TokenEnv != "" {
		token := os.Getenv(c.TokenEnv)
		if token == "" || len(token) > 8192 || strings.ContainsAny(token, "\r\n") {
			return nil, errors.New("missing or invalid telemetry credential")
		}
		headers["Authorization"] = "Bearer " + token
	}
	timeout := time.Duration(c.ExportTimeoutSeconds) * time.Second
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}, DialContext: (&net.Dialer{Timeout: timeout}).DialContext, TLSHandshakeTimeout: timeout, ResponseHeaderTimeout: timeout, IdleConnTimeout: 30 * time.Second, MaxIdleConns: 2, MaxIdleConnsPerHost: 2, MaxConnsPerHost: 2, MaxResponseHeaderBytes: 16 << 10, DisableCompression: true}
	client := &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("telemetry redirect rejected") }}
	base := strings.TrimRight(c.Endpoint, "/")
	traceExporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(base+"/v1/traces"), otlptracehttp.WithHTTPClient(client), otlptracehttp.WithHeaders(headers), otlptracehttp.WithCompression(otlptracehttp.NoCompression), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
	if err != nil {
		transport.CloseIdleConnections()
		return nil, errors.New("cannot initialize trace exporter")
	}
	metricExporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(base+"/v1/metrics"), otlpmetrichttp.WithHTTPClient(client), otlpmetrichttp.WithHeaders(headers), otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression), otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}), otlpmetrichttp.WithTemporalitySelector(sdkmetric.DefaultTemporalitySelector), otlpmetrichttp.WithAggregationSelector(sdkmetric.DefaultAggregationSelector))
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		transport.CloseIdleConnections()
		return nil, errors.New("cannot initialize metric exporter")
	}
	res := resource.NewSchemaless(attribute.String("service.name", service), attribute.String("service.instance.id", uuid.NewString()))
	runtime.traces = sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithSampler(sdktrace.TraceIDRatioBased(c.TraceSampleRatio)), sdktrace.WithRawSpanLimits(sdktrace.SpanLimits{AttributeCountLimit: 16, AttributeValueLengthLimit: 256, AttributeValueDepthLimit: 1}), sdktrace.WithBatcher(safeTraceExporter{SpanExporter: traceExporter, resource: res}, sdktrace.WithMaxQueueSize(2048), sdktrace.WithMaxExportBatchSize(512), sdktrace.WithBatchTimeout(time.Second), sdktrace.WithExportTimeout(timeout)))
	runtime.Metrics = sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithCardinalityLimit(1024), sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(safeMetricExporter{Exporter: metricExporter, resource: res}, sdkmetric.WithInterval(time.Duration(c.MetricIntervalSeconds)*time.Second), sdkmetric.WithTimeout(timeout))))
	runtime.transport = transport
	runtime.Options = model.TelemetryOptions{TracerProvider: runtime.traces, MeterProvider: runtime.Metrics, Logger: logger}
	return runtime, nil
}
func (r *RuntimeTelemetry) Shutdown(ctx context.Context) error {
	if r.traces == nil {
		return nil
	}
	traceErr := r.traces.Shutdown(ctx)
	metricErr := r.Metrics.Shutdown(ctx)
	r.transport.CloseIdleConnections()
	if traceErr != nil || metricErr != nil {
		return errors.New("telemetry shutdown incomplete")
	}
	return nil
}
func (r *RuntimeTelemetry) Close(logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if r.Shutdown(ctx) != nil && logger != nil {
		logger.Warn("telemetry shutdown incomplete")
	}
}
func telemetryError(err error) error {
	if err != nil {
		return errors.New("telemetry export failed")
	}
	return nil
}

// SDK providers merge ambient resource attributes even with WithResource.
// Replace resources at the export boundary to enforce the runtime allowlist.
type safeTraceExporter struct {
	sdktrace.SpanExporter
	resource *resource.Resource
}
type resourceSpan struct {
	sdktrace.ReadOnlySpan
	resource *resource.Resource
}

func (s resourceSpan) Resource() *resource.Resource { return s.resource }

func (e safeTraceExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	safe := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, span := range spans {
		safe[i] = resourceSpan{ReadOnlySpan: span, resource: e.resource}
	}
	return telemetryError(e.SpanExporter.ExportSpans(ctx, safe))
}
func (e safeTraceExporter) Shutdown(ctx context.Context) error {
	return telemetryError(e.SpanExporter.Shutdown(ctx))
}

type safeMetricExporter struct {
	sdkmetric.Exporter
	resource *resource.Resource
}

func (e safeMetricExporter) Export(ctx context.Context, data *metricdata.ResourceMetrics) error {
	safe := *data
	safe.Resource = e.resource
	return telemetryError(e.Exporter.Export(ctx, &safe))
}
func (e safeMetricExporter) ForceFlush(ctx context.Context) error {
	return telemetryError(e.Exporter.ForceFlush(ctx))
}
func (e safeMetricExporter) Shutdown(ctx context.Context) error {
	return telemetryError(e.Exporter.Shutdown(ctx))
}
