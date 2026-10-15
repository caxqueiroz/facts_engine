package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

func (s *Service) checkContextArchive(ctx context.Context, tenant string, minimumRevision int64) (*model.ContextArchiveReceipt, error) {
	policy := s.contextArchive
	if policy == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	progress, err := policy.Progress.ReadOutboxProgress(ctx, model.ConsumerKey{Tenant: tenant, Consumer: model.ArchiveConsumer(policy.Stream)})
	if failure := unavailableDependency(ctx, err); failure != nil {
		return nil, failure
	}
	if progress.AcknowledgedRevision < 0 || progress.LedgerRevision < minimumRevision || progress.LedgerRevision < progress.AcknowledgedRevision || (progress.State != "pending" && progress.State != "running") {
		return nil, model.ErrUnavailable
	}
	lag := progress.LedgerRevision - progress.AcknowledgedRevision
	if lag > policy.MaxPendingRevisions || (lag > 0) != (progress.OldestRecordedAt != nil) {
		return nil, model.ErrUnavailable
	}
	return &model.ContextArchiveReceipt{Stream: policy.Stream, MaxPendingRevisions: policy.MaxPendingRevisions, LedgerRevision: progress.LedgerRevision, AcknowledgedRevision: progress.AcknowledgedRevision, CheckedAt: time.Now().UTC()}, nil
}
