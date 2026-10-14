package telemetry

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ExportIntegrityFailure records a failed check, never an artifact identity.
func (o *Observer) ExportIntegrityFailure(ctx context.Context, kind string) {
	if o == nil {
		return
	}
	switch kind {
	case "audit_bundle", "evidence_content", "archive_chain":
	default:
		kind = "other"
	}
	o.exportIntegrityFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("export.check", kind)))
}
