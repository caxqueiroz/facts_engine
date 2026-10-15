package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgtype"
)

func (t *transaction) guardContextContent(ctx context.Context, id string) error {
	q := ledgerdb.New(t.tx)
	if err := q.LockContextGuard(ctx, ledgerdb.LockContextGuardParams{TenantID: t.tenant, ContextID: id}); err != nil {
		return err
	}
	available, err := q.ContextContentAvailable(ctx, ledgerdb.ContextContentAvailableParams{TenantID: t.tenant, ContextID: id})
	if err != nil {
		return err
	}
	if !available.Valid || !available.Bool {
		return model.ErrPrecondition
	}
	return nil
}
func (t *transaction) DisposeContext(ctx context.Context, r model.ContextDisposalRequest) error {
	inventory, err := ledgerdb.New(t.tx).ContextScopes(ctx, ledgerdb.ContextScopesParams{TenantID: t.tenant, ContextID: r.ContextID})
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
	if err := t.guardContextContent(ctx, r.ContextID); err != nil {
		return err
	}
	manifest, err := t.ContextManifest(ctx, r.ContextID)
	if err != nil {
		return err
	}
	if manifest.PayloadDisposed || manifest.ID != r.ContextID || (manifest.CanonicalVersion != "context-manifest-v1" && manifest.CanonicalVersion != "context-manifest-v2") || manifest.Tenant != t.tenant || model.ContextDigest(manifest) != manifest.Digest || model.BytesDigest(manifest.Rendered) != manifest.RenderedDigest {
		return model.ErrPrecondition
	}
	scopes := map[string]bool{}
	for _, scope := range model.ContextScopes(manifest) {
		scopes[scope] = true
	}
	if len(scopes) != len(r.Bindings) {
		return model.ErrPrecondition
	}
	q := ledgerdb.New(t.tx)
	created, err := q.ContextCreationTime(ctx, ledgerdb.ContextCreationTimeParams{TenantID: t.tenant, ID: r.ContextID})
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
		if policy.RecordClass != "context_payload" || policy.Purpose != r.Purpose || policy.Jurisdiction != b.Jurisdiction {
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
	blocked, err := q.ContextDisposalBlocked(ctx, ledgerdb.ContextDisposalBlockedParams{TenantID: t.tenant, ContextID: pgtype.Text{String: r.ContextID, Valid: true}})
	if err != nil {
		return err
	}
	if !blocked.Valid || blocked.Bool {
		return model.ErrPrecondition
	}
	manifest = model.ContextTombstone(manifest)
	metadata, err := t.encode(manifest)
	if err != nil {
		return err
	}
	if err := q.InsertContextDisposal(ctx, ledgerdb.InsertContextDisposalParams{TenantID: t.tenant, ContextID: r.ContextID, Payload: metadata}); err != nil {
		return err
	}
	n, err := q.DeleteContextPayload(ctx, ledgerdb.DeleteContextPayloadParams{TenantID: t.tenant, ContextID: r.ContextID})
	if err != nil {
		return err
	}
	if n != 1 {
		return model.ErrPrecondition
	}
	return nil
}
