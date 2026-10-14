package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

type contextProgressFunc func(context.Context, model.ConsumerKey) (model.OutboxProgress, error)

func (f contextProgressFunc) ReadOutboxProgress(ctx context.Context, key model.ConsumerKey) (model.OutboxProgress, error) {
	return f(ctx, key)
}

func TestContextArchiveGate(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name     string
		progress model.OutboxProgress
		err      error
		minimum  int64
		allowed  bool
	}{
		{name: "caught up", progress: model.OutboxProgress{LedgerRevision: 10, AcknowledgedRevision: 10, State: "pending"}, minimum: 10, allowed: true},
		{name: "threshold", progress: model.OutboxProgress{LedgerRevision: 10, AcknowledgedRevision: 8, State: "running", OldestRecordedAt: &now}, minimum: 10, allowed: true},
		{name: "behind", progress: model.OutboxProgress{LedgerRevision: 10, AcknowledgedRevision: 7, State: "pending", OldestRecordedAt: &now}},
		{name: "failed", progress: model.OutboxProgress{LedgerRevision: 10, AcknowledgedRevision: 10, State: "failed"}},
		{name: "wrong domain", progress: model.OutboxProgress{LedgerRevision: 9, AcknowledgedRevision: 9, State: "pending"}, minimum: 10},
		{name: "invalid offset", progress: model.OutboxProgress{LedgerRevision: 10, AcknowledgedRevision: 11, State: "pending"}},
		{name: "missing head", progress: model.OutboxProgress{LedgerRevision: 10, AcknowledgedRevision: 8, State: "pending"}},
		{name: "unavailable", err: errors.New("PRIVATE-storage-error")},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &Service{contextArchive: &model.ContextArchiveOptions{Stream: "bank-archive", MaxPendingRevisions: 2, Progress: contextProgressFunc(func(_ context.Context, key model.ConsumerKey) (model.OutboxProgress, error) {
				if key.Tenant != "tenant" || key.Consumer != model.ArchiveConsumer("bank-archive") {
					t.Fatal("archive binding", key)
				}
				return test.progress, test.err
			})}}
			receipt, err := s.checkContextArchive(context.Background(), "tenant", test.minimum)
			if test.allowed {
				if err != nil || receipt == nil || receipt.Stream != "bank-archive" || receipt.CheckedAt.IsZero() || receipt.AcknowledgedRevision != test.progress.AcknowledgedRevision {
					t.Fatal(receipt, err)
				}
			} else if receipt != nil || err != model.ErrUnavailable {
				t.Fatal("failed open or exposed storage error", receipt, err)
			}
		})
	}
}
func TestContextArchiveCancellationAndOptionValidation(t *testing.T) {
	reader := contextProgressFunc(func(ctx context.Context, _ model.ConsumerKey) (model.OutboxProgress, error) {
		<-ctx.Done()
		return model.OutboxProgress{}, ctx.Err()
	})
	s := &Service{contextArchive: &model.ContextArchiveOptions{Stream: "archive", Progress: reader}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.checkContextArchive(ctx, "tenant", 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, policy := range []*model.ContextArchiveOptions{{Stream: "archive"}, {Stream: "bad/id", Progress: reader}, {Stream: "archive", Progress: reader, MaxPendingRevisions: -1}} {
		if _, err := NewWithOptions(&observedLedger{}, &observedAuth{}, observedEvidence{}, model.EngineOptions{ContextArchive: policy}); !errors.Is(err, model.ErrInvalid) {
			t.Fatal(err)
		}
	}
	policy := &model.ContextArchiveOptions{Stream: "archive", Progress: reader, MaxPendingRevisions: 2}
	configured, err := NewWithOptions(&observedLedger{}, &observedAuth{}, observedEvidence{}, model.EngineOptions{ContextArchive: policy})
	if err != nil {
		t.Fatal(err)
	}
	policy.MaxPendingRevisions = 999
	if configured.contextArchive.MaxPendingRevisions != 2 {
		t.Fatal("host mutated established policy")
	}
}
