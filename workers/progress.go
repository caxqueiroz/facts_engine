package workers

import (
	"context"
	"math"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ProgressTarget identifies a trusted, host-configured consumer. Kind is one of
// projection, archive, audit, extraction or refresh, never an arbitrary label.
type ProgressTarget struct {
	Key  model.ConsumerKey
	Kind string
}

// ProgressMonitor aggregates configured consumers by kind during SDK collection.
// Use one monitor per meter provider to avoid overlapping gauge series.
// Close unregisters the callback; the host owns the reader and meter provider.
type ProgressMonitor struct{ registration metric.Registration }

func NewProgressMonitor(reader model.OutboxProgressReader, targets []ProgressTarget, provider metric.MeterProvider, timeout time.Duration) (*ProgressMonitor, error) {
	if reader == nil || provider == nil || len(targets) == 0 || len(targets) > 256 || timeout < time.Millisecond || timeout > 30*time.Second {
		return nil, model.ErrInvalid
	}
	targets = append([]ProgressTarget(nil), targets...)
	seen := map[model.ConsumerKey]bool{}
	for _, target := range targets {
		if !model.ValidID(target.Key.Tenant) || !model.ValidID(target.Key.Consumer) || seen[target.Key] {
			return nil, model.ErrInvalid
		}
		seen[target.Key] = true
		switch target.Kind {
		case "projection", "archive", "audit", "extraction", "refresh":
		default:
			return nil, model.ErrInvalid
		}
	}
	meter := provider.Meter("github.com/caxqueiroz/facts_engine/workers")
	pending, err := meter.Int64ObservableGauge("facts.outbox.pending_revisions", metric.WithDescription("Sum of unacknowledged revisions across monitored consumers by kind"))
	if err != nil {
		return nil, model.ErrInvalid
	}
	oldest, err := meter.Float64ObservableGauge("facts.outbox.oldest_pending_age", metric.WithUnit("s"), metric.WithDescription("Maximum age of the next unacknowledged event across monitored consumers by kind"))
	if err != nil {
		return nil, model.ErrInvalid
	}
	failed, err := meter.Int64ObservableGauge("facts.outbox.failed_consumers")
	if err != nil {
		return nil, model.ErrInvalid
	}
	consumers, err := meter.Int64ObservableGauge("facts.outbox.monitored_consumers")
	if err != nil {
		return nil, model.ErrInvalid
	}
	registration, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		type aggregate struct {
			pending, failed, consumers int64
			age                        float64
		}
		values := map[string]aggregate{}
		for _, target := range targets {
			progress, err := reader.ReadOutboxProgress(ctx, target.Key)
			// Avoid forwarding storage errors containing identifiers to SDK diagnostics.
			if err != nil {
				return model.ErrUnavailable
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if progress.AcknowledgedRevision < 0 || progress.LedgerRevision < progress.AcknowledgedRevision {
				return model.ErrPrecondition
			}
			lag := progress.LedgerRevision - progress.AcknowledgedRevision
			if (lag > 0) != (progress.OldestRecordedAt != nil) {
				return model.ErrPrecondition
			}
			value := values[target.Kind]
			if lag > math.MaxInt64-value.pending {
				return model.ErrPrecondition
			}
			value.pending += lag
			value.consumers++
			switch progress.State {
			case "pending", "running":
			case "failed":
				value.failed++
			default:
				return model.ErrPrecondition
			}
			if progress.OldestRecordedAt != nil {
				if progress.OldestRecordedAt.IsZero() {
					return model.ErrPrecondition
				}
				value.age = max(value.age, time.Since(*progress.OldestRecordedAt).Seconds())
			}
			values[target.Kind] = value
		}
		// Observe only after every target succeeded, never a partial healthy total.
		for kind, value := range values {
			attrs := metric.WithAttributes(attribute.String("worker.kind", kind))
			observer.ObserveInt64(pending, value.pending, attrs)
			observer.ObserveFloat64(oldest, value.age, attrs)
			observer.ObserveInt64(failed, value.failed, attrs)
			observer.ObserveInt64(consumers, value.consumers, attrs)
		}
		return nil
	}, pending, oldest, failed, consumers)
	if err != nil {
		return nil, model.ErrInvalid
	}
	return &ProgressMonitor{registration: registration}, nil
}

func (m *ProgressMonitor) Close() error { return m.registration.Unregister() }
