package postgres

import (
	"context"
	"errors"
	"math"
	"sort"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (t *transaction) PrepareProjectionCleanup(ctx context.Context, r model.ProjectionCleanupRequest) (model.ProjectionCleanupPlan, error) {
	empty := model.ProjectionCleanupPlan{}
	if (r.Kind != "fact" && r.Kind != "memory") || !model.ValidID(r.Scope) || !model.ValidID(r.RecordID) || !model.ValidID(r.VersionID) || !model.ValidID(r.Generation) || !model.ValidID(r.DisposalEventID) || len(r.Bindings) < 1 || len(r.Bindings) > 256 {
		return empty, model.ErrInvalid
	}
	bindings := append([]model.VersionRetentionBinding(nil), r.Bindings...)
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Scope < bindings[j].Scope })
	for i, b := range bindings {
		if !model.ValidID(b.Scope) || (i > 0 && bindings[i-1].Scope == b.Scope) {
			return empty, model.ErrInvalid
		}
		if err := t.lockRetentionScope(ctx, b.Scope); err != nil {
			return empty, err
		}
	}
	q := ledgerdb.New(t.tx)
	capable, err := q.CanAcknowledgeProjectionCleanup(ctx)
	if err != nil {
		return empty, err
	}
	if !capable {
		return empty, model.ErrDenied
	}
	row, err := q.GetCleanupDisposalEvent(ctx, ledgerdb.GetCleanupDisposalEventParams{TenantID: t.tenant, ID: r.DisposalEventID})
	if errors.Is(err, pgx.ErrNoRows) {
		return empty, model.ErrDenied
	}
	if err != nil {
		return empty, err
	}
	if row.ScopeID != r.Scope || row.TargetID != r.VersionID || row.Kind != "dispose_"+r.Kind+"_payload" {
		return empty, model.ErrDenied
	}
	var event model.RetentionEvent
	if err = t.decode(row.Payload, &event); err != nil {
		return empty, err
	}
	old := event.VersionDisposal
	if old == nil || event.ID != r.DisposalEventID || event.Tenant != t.tenant || event.Caller.Tenant != t.tenant || event.Scope != row.ScopeID || event.TargetID != row.TargetID || event.Kind != row.Kind || event.LedgerRevision != row.LedgerRevision || event.LedgerRevision < 1 {
		return empty, model.ErrPrecondition
	}
	if old.Scope != r.Scope || old.RecordID != r.RecordID || old.VersionID != r.VersionID || old.Kind != r.Kind || old.Purpose != r.Purpose || event.Command.Purpose != r.Purpose {
		return empty, model.ErrDenied
	}
	oldBindings := append([]model.VersionRetentionBinding(nil), old.Bindings...)
	sort.Slice(oldBindings, func(i, j int) bool { return oldBindings[i].Scope < oldBindings[j].Scope })
	if model.Digest(oldBindings) != model.Digest(bindings) {
		return empty, model.ErrDenied
	}
	for _, b := range bindings {
		p, err := t.RetentionPolicy(ctx, b.Scope, b.PolicyID)
		if err != nil {
			return empty, err
		}
		if p.ID != b.PolicyID || p.Scope != b.Scope || p.RecordClass != r.Kind+"_payload" || p.Purpose != r.Purpose || p.Jurisdiction != b.Jurisdiction || p.MinimumAgeSeconds < 0 || p.MinimumAgeSeconds > 100*366*86400 {
			return empty, model.ErrPrecondition
		}
		holds, err := q.ListActiveLegalHolds(ctx, ledgerdb.ListActiveLegalHoldsParams{TenantID: t.tenant, ScopeID: b.Scope})
		if err != nil {
			return empty, err
		}
		if len(holds) > 0 {
			return empty, model.ErrPrecondition
		}
	}
	if err = q.LockProjectionRetentionGuard(ctx, t.tenant); err != nil {
		return empty, err
	}
	job, err := q.GetProjectionCleanupJob(ctx, ledgerdb.GetProjectionCleanupJobParams{TenantID: t.tenant, Kind: r.Kind, VersionID: r.VersionID})
	if err != nil {
		return empty, translate(err)
	}
	if job.InventoryEpoch < 1 || !job.CreatedAt.Valid || job.CreatedAt.InfinityModifier != pgtype.Finite {
		return empty, model.ErrPrecondition
	}
	targets, err := q.ListProjectionCleanupTargets(ctx, ledgerdb.ListProjectionCleanupTargetsParams{TenantID: t.tenant, Kind: r.Kind, VersionID: r.VersionID})
	if err != nil {
		return empty, err
	}
	if len(targets) > 10000 || len(targets) != int(job.TargetCount) {
		return empty, model.ErrPrecondition
	}
	found := false
	for _, target := range targets {
		if target.Generation == r.Generation {
			if target.AcknowledgementID.Valid || target.CompletedAt.Valid {
				return empty, model.ErrPrecondition
			}
			found = true
		}
	}
	if !found {
		return empty, model.ErrDenied
	}
	if err = t.lockVersionContent(ctx, r.Kind == "memory", r.VersionID); err != nil {
		return empty, err
	}
	if err = t.verifyCleanupTombstone(ctx, r); err != nil {
		return empty, err
	}
	plan := model.ProjectionCleanupPlan{Target: model.ProjectionDisposal{Tenant: t.tenant, Kind: r.Kind, VersionID: r.VersionID, Generation: r.Generation}}
	state, err := t.generation(ctx, model.GenerationKey{Tenant: t.tenant, Generation: r.Generation})
	if errors.Is(err, pgx.ErrNoRows) {
		return plan, nil
	}
	if err != nil {
		return empty, err
	}
	switch state.State {
	case "building", "active", "resuming":
	case "retired", "abandoned":
		active, err := t.activeProjection(ctx)
		if err != nil {
			return empty, err
		}
		if state.Fence < 1 || state.Fence == math.MaxInt64 || active.Generation == r.Generation {
			return empty, model.ErrPrecondition
		}
		plan.TerminalFence = state.Fence
	default:
		return empty, model.ErrPrecondition
	}
	return plan, nil
}

func (t *transaction) verifyCleanupTombstone(ctx context.Context, r model.ProjectionCleanupRequest) error {
	q := ledgerdb.New(t.tx)
	if r.Kind == "fact" {
		payload, err := q.GetFactVersionDisposalPayload(ctx, ledgerdb.GetFactVersionDisposalPayloadParams{TenantID: t.tenant, VersionID: r.VersionID})
		if err != nil {
			return err
		}
		var v model.Version
		if len(payload) == 0 || t.decodeProjectionTombstone(payload, &v) != nil || !v.PayloadDisposed || v.ID != r.VersionID || v.RecordID != r.RecordID || v.Proposal.Key.Scope != r.Scope || v.Proposer.Tenant != t.tenant || model.Digest(v) != model.Digest(model.VersionTombstone(v)) {
			return model.ErrPrecondition
		}
	} else {
		payload, err := q.GetMemoryVersionDisposalPayload(ctx, ledgerdb.GetMemoryVersionDisposalPayloadParams{TenantID: t.tenant, VersionID: r.VersionID})
		if err != nil {
			return err
		}
		var v model.MemoryVersion
		if len(payload) == 0 || t.decodeProjectionTombstone(payload, &v) != nil || !v.PayloadDisposed || v.ID != r.VersionID || v.RecordID != r.RecordID || v.Proposal.Key.Scope != r.Scope || v.Proposer.Tenant != t.tenant || model.Digest(v) != model.Digest(model.MemoryVersionTombstone(v)) {
			return model.ErrPrecondition
		}
	}
	return nil
}

var _ model.ProjectionCleanupTx = (*transaction)(nil)
