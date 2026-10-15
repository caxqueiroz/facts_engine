package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) guardSnapshotContent(ctx context.Context, id string) error {
	q := ledgerdb.New(t.tx)
	if err := q.LockSnapshotGuard(ctx, ledgerdb.LockSnapshotGuardParams{TenantID: t.tenant, SnapshotID: id}); err != nil {
		return err
	}
	available, err := q.SnapshotContentAvailable(ctx, ledgerdb.SnapshotContentAvailableParams{TenantID: t.tenant, SnapshotID: id})
	if err != nil {
		return err
	}
	if !available.Valid || !available.Bool {
		return model.ErrPrecondition
	}
	return nil
}
func (t *transaction) DisposeSnapshot(ctx context.Context, r model.SnapshotDisposalRequest) error {
	inventory, err := ledgerdb.New(t.tx).SnapshotScopes(ctx, ledgerdb.SnapshotScopesParams{TenantID: t.tenant, SnapshotID: r.SnapshotID})
	if err != nil {
		return err
	}
	if len(inventory) != len(r.Bindings) {
		return model.ErrDenied
	}
	allowed := map[string]bool{}
	for _, b := range r.Bindings {
		allowed[b.Scope] = true
	}
	for _, scope := range inventory {
		if !allowed[scope] {
			return model.ErrDenied
		}
	}

	for _, b := range r.Bindings {
		if err := t.lockRetentionScope(ctx, b.Scope); err != nil {
			return err
		}
	}
	if err := t.guardSnapshotContent(ctx, r.SnapshotID); err != nil {
		return err
	}
	snapshot, err := t.Snapshot(ctx, r.SnapshotID)
	if err != nil {
		return err
	}
	if snapshot.PayloadDisposed || snapshot.Tenant != t.tenant || model.SnapshotDigest(snapshot) != snapshot.Digest {
		return model.ErrPrecondition
	}
	scopes := map[string]bool{}
	for _, item := range snapshot.Items {
		scopes[item.Selector.Scope] = true
	}
	if len(scopes) != len(r.Bindings) {
		return model.ErrPrecondition
	}
	q := ledgerdb.New(t.tx)
	created, err := q.SnapshotCreationTime(ctx, ledgerdb.SnapshotCreationTimeParams{TenantID: t.tenant, ID: r.SnapshotID})
	if err != nil {
		return err
	}
	for _, b := range r.Bindings {
		if !scopes[b.Scope] {
			return model.ErrDenied
		}
		policy, err := t.RetentionPolicy(ctx, b.Scope, b.PolicyID)
		if err != nil {
			return err
		}
		if policy.RecordClass != "snapshot_payload" || policy.Purpose != r.Purpose || policy.Jurisdiction != b.Jurisdiction {
			return model.ErrPrecondition
		}
		holds, err := q.ListActiveLegalHolds(ctx, ledgerdb.ListActiveLegalHoldsParams{TenantID: t.tenant, ScopeID: b.Scope})
		if err != nil {
			return err
		}
		if len(holds) > 0 {
			return model.ErrPrecondition
		}
		mature, err := q.EvidenceAgeMature(ctx, ledgerdb.EvidenceAgeMatureParams{RegisteredAt: created, MinimumAgeSeconds: policy.MinimumAgeSeconds})
		if err != nil {
			return err
		}
		if !mature {
			return model.ErrPrecondition
		}
	}
	blocked, err := q.SnapshotDisposalBlocked(ctx, ledgerdb.SnapshotDisposalBlockedParams{TenantID: t.tenant, SnapshotID: r.SnapshotID})
	if err != nil {
		return err
	}
	if !blocked.Valid || blocked.Bool {
		return model.ErrPrecondition
	}
	snapshot.PayloadDisposed = true
	for i := range snapshot.Items {
		snapshot.Items[i].Resolution.Value = nil
	}
	metadata, err := t.encode(snapshot)
	if err != nil {
		return err
	}
	if err := q.InsertSnapshotDisposal(ctx, ledgerdb.InsertSnapshotDisposalParams{TenantID: t.tenant, SnapshotID: r.SnapshotID, Payload: metadata}); err != nil {
		return err
	}
	n, err := q.DeleteSnapshotPayload(ctx, ledgerdb.DeleteSnapshotPayloadParams{TenantID: t.tenant, SnapshotID: r.SnapshotID})
	if err != nil {
		return err
	}
	if n != 1 {
		return model.ErrPrecondition
	}
	return nil
}
