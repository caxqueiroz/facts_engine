package model

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
)

// TelemetryOptions injects host-owned providers. Nil providers are local no-ops,
// never global defaults. The host owns exporters, sampling, flush and shutdown.
type TelemetryOptions struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Logger         *slog.Logger
}
