package postgres

import (
	"context"
	"errors"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (t *transaction) DisposeAttestation(ctx context.Context, r model.AttestationDisposalRequest) (int64, error) {
	q := ledgerdb.New(t.tx)
	var scope, record, version string
	var revision int64
	var created pgtype.Timestamptz
	var err error
	if r.Kind == "fact" {
		h, err := q.FactAttestationDisposalHeader(ctx, ledgerdb.FactAttestationDisposalHeaderParams{TenantID: t.tenant, ID: r.AttestationID})
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, model.ErrDenied
		}
		if err != nil {
			return 0, err
		}
		scope, record, version = h.ScopeID, h.RecordID, h.VersionID
		if !h.LedgerRevision.Valid {
			return 0, model.ErrPrecondition
		}
		revision, created = h.LedgerRevision.Int64, h.RecordedAt
	} else {
		h, err := q.MemoryAttestationDisposalHeader(ctx, ledgerdb.MemoryAttestationDisposalHeaderParams{TenantID: t.tenant, ID: r.AttestationID})
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, model.ErrDenied
		}
		if err != nil {
			return 0, err
		}
		scope, record, version, revision, created = h.ScopeID, h.RecordID, h.VersionID, h.LedgerRevision, h.RecordedAt
	}
	if scope != r.Scope || record != r.RecordID || version != r.VersionID || !created.Valid || created.InfinityModifier != pgtype.Finite || revision < 1 {
		return 0, model.ErrDenied
	}
	if err := t.lockRetentionScope(ctx, scope); err != nil {
		return 0, err
	}
	if err := q.LockAttestationGuard(ctx, ledgerdb.LockAttestationGuardParams{TenantID: t.tenant, AttestationID: r.AttestationID}); err != nil {
		return 0, err
	}
	var payload []byte
	if r.Kind == "fact" {
		payload, err = q.GetFactAttestationPayload(ctx, ledgerdb.GetFactAttestationPayloadParams{TenantID: t.tenant, ID: r.AttestationID})
	} else {
		payload, err = q.GetMemoryAttestationPayload(ctx, ledgerdb.GetMemoryAttestationPayloadParams{TenantID: t.tenant, ID: r.AttestationID})
	}
	if err != nil {
		return 0, err
	}
	if len(payload) == 0 {
		return 0, model.ErrPrecondition
	}
	var attestation model.Attestation
	if err := t.decode(payload, &attestation); err != nil {
		return 0, err
	}
	if attestation.PayloadDisposed || attestation.ID != r.AttestationID || attestation.Scope != scope || attestation.RecordID != record || attestation.VersionID != version || attestation.Attestor.Tenant != t.tenant || !model.ValidID(attestation.Attestor.Principal) {
		return 0, model.ErrPrecondition
	}
	policy, err := t.RetentionPolicy(ctx, scope, r.PolicyID)
	if err != nil {
		return 0, err
	}
	if policy.ID != r.PolicyID || policy.Scope != scope || policy.RecordClass != "attestation_payload" || policy.Purpose != r.Purpose || policy.Jurisdiction != r.Jurisdiction {
		return 0, model.ErrPrecondition
	}
	holds, err := q.ListActiveLegalHolds(ctx, ledgerdb.ListActiveLegalHoldsParams{TenantID: t.tenant, ScopeID: scope})
	if err != nil {
		return 0, err
	}
	if len(holds) > 0 {
		return 0, model.ErrPrecondition
	}
	mature, err := q.EvidenceAgeMature(ctx, ledgerdb.EvidenceAgeMatureParams{RegisteredAt: created, MinimumAgeSeconds: policy.MinimumAgeSeconds})
	if err != nil {
		return 0, err
	}
	if !mature {
		return 0, model.ErrPrecondition
	}
	blocked, err := q.AttestationDisposalBlocked(ctx, ledgerdb.AttestationDisposalBlockedParams{TenantID: t.tenant, ScopeID: scope, LedgerRevision: revision})
	if err != nil {
		return 0, err
	}
	if !blocked.Valid || blocked.Bool {
		return 0, model.ErrPrecondition
	}
	var streamRevision int64
	if r.Kind == "fact" {
		streamRevision, err = q.LockRecord(ctx, ledgerdb.LockRecordParams{TenantID: t.tenant, ID: record})
		if err != nil {
			return 0, err
		}
	} else {
		owner, err := q.LockMemoryRecord(ctx, ledgerdb.LockMemoryRecordParams{TenantID: t.tenant, ID: record})
		if err != nil {
			return 0, err
		}
		if owner.Kind != "memory" {
			return 0, model.ErrPrecondition
		}
		streamRevision = owner.StreamRevision
	}
	marker, err := t.encode(model.AttestationTombstone(attestation))
	if err != nil {
		return 0, err
	}
	if r.Kind == "fact" {
		if err = q.InsertFactAttestationDisposal(ctx, ledgerdb.InsertFactAttestationDisposalParams{TenantID: t.tenant, AttestationID: r.AttestationID, Payload: marker}); err != nil {
			return 0, err
		}
	} else if err = q.InsertMemoryAttestationDisposal(ctx, ledgerdb.InsertMemoryAttestationDisposalParams{TenantID: t.tenant, AttestationID: r.AttestationID, Payload: marker}); err != nil {
		return 0, err
	}
	var removed int64
	if r.Kind == "fact" {
		removed, err = q.DeleteFactAttestationPayload(ctx, ledgerdb.DeleteFactAttestationPayloadParams{TenantID: t.tenant, AttestationID: r.AttestationID})
	} else {
		removed, err = q.DeleteMemoryAttestationPayload(ctx, ledgerdb.DeleteMemoryAttestationPayloadParams{TenantID: t.tenant, AttestationID: r.AttestationID})
	}
	if err != nil {
		return 0, err
	}
	if removed != 1 {
		return 0, model.ErrPrecondition
	}
	if err = t.SetRevision(ctx, record, streamRevision+1); err != nil {
		return 0, err
	}
	return streamRevision + 1, nil
}
