package postgres

import (
	"context"
	"errors"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
)

func (t *transaction) DisposeOrphanEvidence(ctx context.Context, r model.OrphanEvidenceDisposalRequest) error {
	// Scope and object epochs serialize holds and registrations, including aliases
	// in other scopes. A concurrent write invalidates a stale transaction snapshot.
	if err := t.lockRetentionScope(ctx, r.Scope); err != nil {
		return err
	}
	if err := t.lockEvidenceObject(ctx, r.Object); err != nil {
		return err
	}
	q := ledgerdb.New(t.tx)
	upload, err := q.GetPendingEvidenceUpload(ctx, ledgerdb.GetPendingEvidenceUploadParams{TenantID: t.tenant, Locator: r.Object.Locator, ObjectVersion: r.Object.Version})
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrPrecondition
	}
	if err != nil {
		return err
	}
	if upload.ScopeID != r.Scope {
		return model.ErrDenied
	}
	if upload.PolicyID != r.PolicyID || upload.ByteSize != r.Object.Size {
		return model.ErrPrecondition
	}
	policy, err := t.RetentionPolicy(ctx, r.Scope, r.PolicyID)
	if err != nil {
		return err
	}
	if policy.RecordClass != "orphan_evidence_bytes" || policy.Purpose != r.Purpose || policy.Jurisdiction != r.Jurisdiction {
		return model.ErrPrecondition
	}
	holds, err := q.ListActiveLegalHolds(ctx, ledgerdb.ListActiveLegalHoldsParams{TenantID: t.tenant, ScopeID: r.Scope})
	if err != nil {
		return err
	}
	if len(holds) > 0 {
		return model.ErrPrecondition
	}
	registered, err := q.HasRegisteredEvidenceObject(ctx, ledgerdb.HasRegisteredEvidenceObjectParams{TenantID: t.tenant, Locator: r.Object.Locator, ObjectVersion: r.Object.Version})
	if err != nil {
		return err
	}
	if registered {
		return model.ErrPrecondition
	}
	// Encrypted legacy metadata can alias any object; require a complete inventory.
	legacy, err := q.HasUnboundLegacyEvidence(ctx, ledgerdb.HasUnboundLegacyEvidenceParams{TenantID: t.tenant, ID: ""})
	if err != nil {
		return err
	}
	if legacy {
		return model.ErrPrecondition
	}
	mature, err := q.EvidenceAgeMature(ctx, ledgerdb.EvidenceAgeMatureParams{RegisteredAt: upload.UploadedAt, MinimumAgeSeconds: policy.MinimumAgeSeconds})
	if err != nil {
		return err
	}
	if !mature {
		return model.ErrPrecondition
	}
	return t.disposeEvidenceObject(ctx, model.Evidence{Object: r.Object})
}

var _ model.OrphanEvidenceDisposalTx = (*transaction)(nil)
