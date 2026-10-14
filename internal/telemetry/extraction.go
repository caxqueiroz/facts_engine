package telemetry

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ExtractionResult observes successful result persistence, outside transaction
// retry callbacks. It is operational telemetry, not an exactly-once audit count.
func (o *Observer) ExtractionResult(ctx context.Context, kind, outcome string, proposals, issues int) {
	if o == nil {
		return
	}
	switch kind {
	case "extraction", "memory_extraction", "memory_refresh":
	default:
		kind = "other"
	}
	switch outcome {
	case "completed", "response_limit", "provider_unavailable", "invalid_model_metadata", "invalid_response", "authorization_changed", "lease_exhausted_response_unavailable":
	default:
		outcome = "other"
	}
	attrs := metric.WithAttributes(attribute.String("extraction.kind", kind), attribute.String("extraction.outcome", outcome))
	o.extractionResults.Add(ctx, 1, attrs)
	if proposals > 0 {
		o.extractionProposals.Add(ctx, int64(proposals), attrs)
	}
	if issues > 0 {
		o.extractionIssues.Add(ctx, int64(issues), attrs)
	}
}

// ExtractionRejected observes a durable pre-provider input-limit rejection.
func (o *Observer) ExtractionRejected(ctx context.Context, kind string) {
	if o == nil {
		return
	}
	switch kind {
	case "extraction", "memory_extraction", "memory_refresh":
	default:
		kind = "other"
	}
	o.extractionRejected.Add(ctx, 1, metric.WithAttributes(attribute.String("extraction.kind", kind), attribute.String("extraction.outcome", "input_limit")))
}
