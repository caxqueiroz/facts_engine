package workers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type progressReaderFunc func(context.Context, model.ConsumerKey) (model.OutboxProgress, error)

func (f progressReaderFunc) ReadOutboxProgress(ctx context.Context, key model.ConsumerKey) (model.OutboxProgress, error) {
	return f(ctx, key)
}

func TestProgressMonitorAggregationPrivacyAndFailure(t *testing.T) {
	ctx := context.Background()
	manual := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(manual))
	defer func() { _ = provider.Shutdown(ctx) }()
	minuteAgo := time.Now().Add(-time.Minute)
	twoMinutesAgo := time.Now().Add(-2 * time.Minute)
	snapshots := map[string]model.OutboxProgress{
		"PRIVATE-one":     {LedgerRevision: 10, AcknowledgedRevision: 7, State: "running", OldestRecordedAt: &minuteAgo},
		"PRIVATE-two":     {LedgerRevision: 20, AcknowledgedRevision: 15, State: "failed", OldestRecordedAt: &twoMinutesAgo},
		"PRIVATE-archive": {LedgerRevision: 10, AcknowledgedRevision: 10, State: "pending"},
	}
	var fail bool
	reader := progressReaderFunc(func(_ context.Context, key model.ConsumerKey) (model.OutboxProgress, error) {
		if fail && key.Consumer == "PRIVATE-two" {
			return model.OutboxProgress{}, errors.New("PRIVATE-storage-error")
		}
		return snapshots[key.Consumer], nil
	})
	targets := []ProgressTarget{
		{Key: model.ConsumerKey{Tenant: "PRIVATE-tenant", Consumer: "PRIVATE-one"}, Kind: "projection"},
		{Key: model.ConsumerKey{Tenant: "PRIVATE-tenant", Consumer: "PRIVATE-two"}, Kind: "projection"},
		{Key: model.ConsumerKey{Tenant: "PRIVATE-tenant", Consumer: "PRIVATE-archive"}, Kind: "archive"},
	}
	monitor, err := NewProgressMonitor(reader, targets, provider, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = monitor.Close() }()
	targets[0].Kind = "PRIVATE-mutated"
	collect := func() (metricdata.ResourceMetrics, error) {
		var data metricdata.ResourceMetrics
		err := manual.Collect(ctx, &data)
		return data, err
	}
	data, err := collect()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch gauge := m.Data.(type) {
			case metricdata.Gauge[int64]:
				counts[m.Name] = map[string]int64{}
				for _, p := range gauge.DataPoints {
					if p.Attributes.Len() != 1 {
						t.Fatal(p.Attributes)
					}
					kind, _ := p.Attributes.Value("worker.kind")
					counts[m.Name][kind.AsString()] = p.Value
				}
			case metricdata.Gauge[float64]:
				for _, p := range gauge.DataPoints {
					kind, _ := p.Attributes.Value("worker.kind")
					if kind.AsString() == "projection" && (p.Value < 120 || p.Value > 130) {
						t.Fatal(p.Value)
					}
					if kind.AsString() == "archive" && p.Value != 0 {
						t.Fatal(p.Value)
					}
				}
			}
		}
	}
	if counts["facts.outbox.pending_revisions"]["projection"] != 8 || counts["facts.outbox.pending_revisions"]["archive"] != 0 || counts["facts.outbox.failed_consumers"]["projection"] != 1 || counts["facts.outbox.monitored_consumers"]["projection"] != 2 {
		t.Fatal(counts)
	}
	encoded, _ := json.Marshal(data)
	if strings.Contains(string(encoded), "PRIVATE") {
		t.Fatal("identifiers in metrics")
	}
	fail = true
	data, err = collect()
	if err == nil || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("collection failure was hidden or leaked", err)
	}
	for _, scope := range data.ScopeMetrics {
		if len(scope.Metrics) != 0 {
			t.Fatal("partial or stale progress published", scope.Metrics)
		}
	}
	fail = false
	snapshots["PRIVATE-one"] = model.OutboxProgress{LedgerRevision: 10, AcknowledgedRevision: 10, State: "pending"}
	snapshots["PRIVATE-two"] = snapshots["PRIVATE-one"]
	data, err = collect()
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "facts.outbox.pending_revisions" {
				for _, p := range m.Data.(metricdata.Gauge[int64]).DataPoints {
					if p.Value != 0 {
						t.Fatal("lag failed to recover", p.Value)
					}
				}
			}
		}
	}
	if err := monitor.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProgressMonitorTimeoutAndValidation(t *testing.T) {
	manual := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(manual))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	target := ProgressTarget{Key: model.ConsumerKey{Tenant: "tenant", Consumer: "consumer"}, Kind: "archive"}
	reader := progressReaderFunc(func(ctx context.Context, _ model.ConsumerKey) (model.OutboxProgress, error) {
		<-ctx.Done()
		return model.OutboxProgress{}, ctx.Err()
	})
	if _, err := NewProgressMonitor(reader, []ProgressTarget{target, target}, provider, time.Second); !errors.Is(err, model.ErrInvalid) {
		t.Fatal(err)
	}
	invalid := target
	invalid.Kind = "PRIVATE-kind"
	if _, err := NewProgressMonitor(reader, []ProgressTarget{invalid}, provider, time.Second); !errors.Is(err, model.ErrInvalid) {
		t.Fatal(err)
	}
	monitor, err := NewProgressMonitor(reader, []ProgressTarget{target}, provider, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = monitor.Close() }()
	var data metricdata.ResourceMetrics
	if err := manual.Collect(context.Background(), &data); err == nil {
		t.Fatal("timeout hidden")
	}
}

func TestProgressMonitorRejectsInconsistentSnapshots(t *testing.T) {
	now := time.Now()
	for _, snapshot := range []model.OutboxProgress{
		{LedgerRevision: 1, AcknowledgedRevision: 2, State: "pending"},
		{LedgerRevision: 2, AcknowledgedRevision: 1, State: "pending"},
		{LedgerRevision: 2, AcknowledgedRevision: 2, State: "pending", OldestRecordedAt: &now},
		{LedgerRevision: 2, AcknowledgedRevision: 2, State: "PRIVATE-invalid-state"},
	} {
		manual := sdkmetric.NewManualReader()
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(manual))
		reader := progressReaderFunc(func(context.Context, model.ConsumerKey) (model.OutboxProgress, error) { return snapshot, nil })
		monitor, err := NewProgressMonitor(reader, []ProgressTarget{{Key: model.ConsumerKey{Tenant: "tenant", Consumer: "consumer"}, Kind: "archive"}}, provider, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var data metricdata.ResourceMetrics
		if err := manual.Collect(context.Background(), &data); err == nil || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("invalid snapshot not rejected safely", err)
		}
		if err := monitor.Close(); err != nil {
			t.Fatal(err)
		}
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
