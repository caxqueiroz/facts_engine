package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) StageMemoryAttestation(ctx context.Context, a model.Attestation) error {
	if err := t.guardEvidenceReference(ctx, a.EvidenceID); err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx, `INSERT INTO attestation_identities VALUES($1,$2,$3,'memory')`, t.tenant, a.ID, a.Scope); err != nil {
		return err
	}
	t.pendingMemoryAttestation = &a
	return nil
}
func (t *transaction) MemoryAttestations(ctx context.Context, version string, revision int64) ([]model.Attestation, error) {
	rows, err := t.tx.Query(ctx, `SELECT a.payload FROM memory_attestations a WHERE a.tenant_id=$1 AND a.version_id=$2 AND ($3::bigint=0 OR a.ledger_revision<=$3) AND NOT EXISTS(SELECT 1 FROM memory_attestation_events w WHERE w.tenant_id=a.tenant_id AND w.attestation_id=a.id AND ($3::bigint=0 OR w.ledger_revision<=$3)) ORDER BY a.id`, t.tenant, version, revision)
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
	if err := t.read(ctx, &a, `SELECT payload FROM memory_attestations WHERE tenant_id=$1 AND id=$2`, t.tenant, id); err != nil {
		return model.CommitResult{}, err
	}
	if a.Scope != scope || a.Attestor.Principal != c.Principal {
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
		payload, err := t.encode(a)
		if err != nil {
			return err
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO memory_attestations(tenant_id,id,scope_id,record_id,version_id,evidence_id,ledger_revision,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, t.tenant, a.ID, a.Scope, a.RecordID, a.VersionID, a.EvidenceID, e.LedgerRevision, payload); err != nil {
			return err
		}
	}
	if t.pendingMemoryWithdrawal != "" {
		if _, err := t.tx.Exec(ctx, `INSERT INTO memory_attestation_events VALUES($1,$2,$3,$4)`, t.tenant, t.pendingMemoryWithdrawal, e.Caller.Principal, e.LedgerRevision); err != nil {
			return err
		}
	}
	return nil
}
