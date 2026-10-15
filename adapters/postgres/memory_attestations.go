package postgres

import (
	"context"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) StageMemoryAttestation(ctx context.Context, a model.Attestation) error {
	if err := t.guardEvidenceReference(ctx, a.EvidenceID); err != nil {
		return err
	}
	if err := ledgerdb.New(t.tx).InsertMemoryAttestationIdentity(ctx, ledgerdb.InsertMemoryAttestationIdentityParams{TenantID: t.tenant, ID: a.ID, ScopeID: a.Scope}); err != nil {
		return err
	}
	if err := t.stageConfirmation(ctx, a, "memory"); err != nil {
		return err
	}
	t.pendingMemoryAttestation = &a
	return nil
}
func (t *transaction) MemoryAttestations(ctx context.Context, version string, revision int64) ([]model.Attestation, error) {
	rows, err := t.tx.Query(ctx, ledgerdb.ListMemoryAttestationPayloads, t.tenant, version, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Attestation{}
	for rows.Next() {
		var payload []byte
		if err = rows.Scan(&payload); err != nil {
			return nil, err
		}
		if len(payload) == 0 {
			return nil, model.ErrPrecondition
		}
		var a model.Attestation
		if err = t.decode(payload, &a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (t *transaction) withdrawMemoryAttestation(ctx context.Context, id, scope string, c model.Caller) (model.CommitResult, error) {
	var a model.Attestation
	payload, err := ledgerdb.New(t.tx).GetMemoryAttestationPayload(ctx, ledgerdb.GetMemoryAttestationPayloadParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return model.CommitResult{}, translate(err)
	}
	if len(payload) == 0 {
		return model.CommitResult{}, model.ErrPrecondition
	}
	if err := t.decode(payload, &a); err != nil {
		return model.CommitResult{}, err
	}
	if a.Scope != scope || a.Attestor != c {
		return model.CommitResult{}, model.ErrDenied
	}
	v, err := t.MemoryVersion(ctx, a.RecordID, a.VersionID)
	if err != nil {
		return model.CommitResult{}, err
	}
	record, err := t.LockMemory(ctx, v.Proposal.Key)
	if err != nil {
		return model.CommitResult{}, err
	}
	t.pendingMemoryWithdrawal = id
	result := model.CommitResult{ID: id, RecordID: record.ID, VersionID: v.ID, RecordKind: "memory", StreamRevision: record.Revision + 1}
	return result, t.SetRevision(ctx, record.ID, result.StreamRevision)
}
func (t *transaction) flushMemoryAttestations(ctx context.Context, e model.Event) error {
	if t.pendingMemoryAttestation != nil {
		a := *t.pendingMemoryAttestation
		at := e.RecordedAt
		a.RecordedAt = &at
		payload, err := t.encode(a)
		if err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertMemoryAttestationHeader(ctx, ledgerdb.InsertMemoryAttestationHeaderParams{TenantID: t.tenant, ID: a.ID, ScopeID: a.Scope, RecordID: a.RecordID, VersionID: a.VersionID, EvidenceID: a.EvidenceID, LedgerRevision: e.LedgerRevision}); err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertMemoryAttestationPayload(ctx, ledgerdb.InsertMemoryAttestationPayloadParams{TenantID: t.tenant, AttestationID: a.ID, Payload: payload}); err != nil {
			return err
		}
	}
	if t.pendingMemoryWithdrawal != "" {
		if err := ledgerdb.New(t.tx).InsertMemoryWithdrawal(ctx, ledgerdb.InsertMemoryWithdrawalParams{TenantID: t.tenant, AttestationID: t.pendingMemoryWithdrawal, Actor: e.Caller.Principal, LedgerRevision: e.LedgerRevision}); err != nil {
			return err
		}
	}
	return nil
}
