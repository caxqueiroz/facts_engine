package services

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) CleanupProjection(ctx context.Context, c model.Caller, r model.ProjectionCleanupRequest) (model.CommitResult, error) {
	if (r.Kind != "fact" && r.Kind != "memory") || !model.ValidID(r.Scope) || !model.ValidID(r.RecordID) || !model.ValidID(r.VersionID) || !model.ValidID(r.Generation) || !model.ValidID(r.DisposalEventID) || r.Reason == "" || len(r.Reason) > 1024 || len(r.Bindings) < 1 || len(r.Bindings) > 256 {
		return model.CommitResult{}, model.ErrInvalid
	}
	r.Bindings = append([]model.VersionRetentionBinding(nil), r.Bindings...)
	sort.Slice(r.Bindings, func(i, j int) bool { return r.Bindings[i].Scope < r.Bindings[j].Scope })
	for i, b := range r.Bindings {
		if !model.ValidID(b.Scope) || !model.ValidID(b.PolicyID) || !model.ValidID(b.Jurisdiction) || (i > 0 && r.Bindings[i-1].Scope == b.Scope) {
			return model.CommitResult{}, model.ErrInvalid
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var receipts []model.AuthorizationDecision
	check := func() error {
		receipts = nil
		for _, b := range r.Bindings {
			d, err := s.authorize(ctx, c, b.Scope, "can_dispose", r.Command)
			if err != nil {
				return err
			}
			receipts = append(receipts, d)
		}
		return nil
	}
	if err := check(); err != nil {
		return model.CommitResult{}, err
	}
	if s.projectionCleanup.Destination == nil {
		return model.CommitResult{}, model.ErrPrecondition
	}
	return s.mutate(ctx, c, r.Scope, "cleanup_projection", "can_dispose", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		cleanup, ok := tx.(model.ProjectionCleanupTx)
		if !ok {
			return model.ErrPrecondition
		}
		retention, err := retentionTx(tx)
		if err != nil {
			return err
		}
		plan, err := cleanup.PrepareProjectionCleanup(ctx, r)
		if err != nil {
			return err
		}
		want := model.ProjectionDisposal{Tenant: c.Tenant, Kind: r.Kind, VersionID: r.VersionID, Generation: r.Generation}
		if plan.Target != want || plan.TerminalFence < 0 {
			return model.ErrPrecondition
		}
		if err = check(); err != nil {
			return err
		}
		if plan.TerminalFence > 0 {
			gate, ok := s.projectionCleanup.Destination.(model.ProjectionCleanupGate)
			if !ok {
				return model.ErrPrecondition
			}
			key := model.GenerationKey{Tenant: c.Tenant, Generation: r.Generation}
			if err = gate.SetProjectionGate(ctx, key, plan.TerminalFence, true); err != nil {
				return cleanupDependencyFailure(ctx, err)
			}
			if err = gate.SealProjectionForDisposal(ctx, key, plan.TerminalFence); err != nil {
				return cleanupDependencyFailure(ctx, err)
			}
		}
		if err = s.projectionCleanup.Destination.DisposeProjection(ctx, plan.Target); err != nil {
			return cleanupDependencyFailure(ctx, err)
		}
		if err = check(); err != nil {
			return err
		}
		event := model.RetentionEvent{ID: id(), Tenant: c.Tenant, Scope: r.Scope, Kind: "cleanup_projection", TargetID: r.VersionID, Caller: c, Command: r.Command, ProjectionCleanup: &r, AdditionalAuthorization: append([]model.AuthorizationDecision(nil), receipts...), Limitations: []string{"acknowledges_one_captured_destination", "structural_metadata_retained", "backups_expire_under_separate_policy", "archives_expire_under_separate_policy"}}
		out.ID = event.ID
		return retention.StageRetention(ctx, event)
	}, mutationCheck{onReceipt: check, onCommit: check})
}

func cleanupDependencyFailure(ctx context.Context, err error) error {
	if interrupted := evidenceDependencyFailure(ctx, err); interrupted != nil {
		return interrupted
	}
	for _, known := range []error{model.ErrDenied, model.ErrInvalid, model.ErrPrecondition, model.ErrRevision} {
		if errors.Is(err, known) {
			return known
		}
	}
	return model.ErrUnavailable
}
