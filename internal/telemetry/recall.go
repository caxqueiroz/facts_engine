package telemetry

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RecallFiltered records completed ledger revalidation, outside retry callbacks.
// Missing versions and ineligible resolutions are distinct from malformed hits,
// duplicate hits, wrong kinds, scope exclusions and disclosure failures.
func (o *Observer) RecallFiltered(ctx context.Context, kind string, missing, ineligible int64) {
	if o == nil {
		return
	}
	if kind != "fact" && kind != "memory" {
		kind = "other"
	}
	for reason, count := range map[string]int64{"missing_version": missing, "ineligible_version": ineligible} {
		if count > 0 {
			o.recallFiltered.Add(ctx, count, metric.WithAttributes(attribute.String("recall.kind", kind), attribute.String("recall.reason", reason)))
		}
	}
}
