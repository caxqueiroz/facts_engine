package postgres

import (
	"context"
	"errors"

	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
)

func (t *transaction) MemoryType(ctx context.Context, scope, subtype, version string) (model.MemoryType, error) {
	var d model.MemoryType
	err := t.read(ctx, &d, `SELECT payload FROM memory_types WHERE tenant_id=$1 AND scope_id=$2 AND subtype=$3 AND version=$4`, t.tenant, scope, subtype, version)
	return d, err
}
func (t *transaction) ActiveMemoryType(ctx context.Context, scope, subtype string) (string, error) {
	var version string
	err := t.tx.QueryRow(ctx, `SELECT version FROM active_memory_types WHERE tenant_id=$1 AND scope_id=$2 AND subtype=$3 FOR SHARE`, t.tenant, scope, subtype).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", model.ErrNotFound
	}
	return version, err
}
func (t *transaction) StageMemoryType(ctx context.Context, d model.MemoryType) error {
	// Lock/update the active pointer before taking the tenant append lock. Its
	// deferred FK is satisfied by the immutable definition flushed with the event.
	_, err := t.tx.Exec(ctx, `INSERT INTO active_memory_types VALUES($1,$2,$3,$4) ON CONFLICT(tenant_id,scope_id,subtype) DO UPDATE SET version=EXCLUDED.version`, t.tenant, d.Scope, d.Subtype, d.Version)
	if err != nil {
		return err
	}
	t.pendingMemoryType = &d
	return nil
}
func (t *transaction) LockMemory(ctx context.Context, k model.MemoryKey) (model.Record, error) {
	id := model.MemoryRecordID(k)
	_, err := t.tx.Exec(ctx, `INSERT INTO records(tenant_id,id,scope_id,subject_id,predicate,discriminator,kind) VALUES($1,$2,$3,'','',$4,'memory') ON CONFLICT DO NOTHING`, t.tenant, id, k.Scope, k.ID)
	if err != nil {
		return model.Record{}, err
	}
	var r model.Record
	err = t.tx.QueryRow(ctx, `SELECT stream_revision,kind FROM records WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, t.tenant, id).Scan(&r.Revision, &r.Kind)
	r.ID = id
	if err == nil && r.Kind != "memory" {
		return r, model.ErrPrecondition
	}
	return r, err
}
func (t *transaction) StageMemoryVersion(ctx context.Context, v model.MemoryVersion) error {
	for _, span := range v.Proposal.Sources {
		if err := t.guardEvidenceReference(ctx, span.EvidenceID); err != nil {
			return err
		}
	}
	t.pendingMemoryVersion = &v
	return nil
}
func (t *transaction) MemoryVersion(ctx context.Context, record, version string) (model.MemoryVersion, error) {
	var v model.MemoryVersion
	err := t.read(ctx, &v, `SELECT payload FROM memory_versions WHERE tenant_id=$1 AND record_id=$2 AND id=$3`, t.tenant, record, version)
	return v, err
}
func (t *transaction) flushMemory(ctx context.Context, e model.Event) error {
	if t.pendingMemoryType != nil {
		d := *t.pendingMemoryType
		payload, err := t.encode(d)
		if err != nil {
			return err
		}
		var from *string
		if d.Migration != nil {
			from = &d.Migration.FromVersion
		}
		_, err = t.tx.Exec(ctx, `INSERT INTO memory_types VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, t.tenant, d.Scope, d.Subtype, d.Version, d.PolicyID, from, e.LedgerRevision, payload)
		if err != nil {
			return err
		}

	}
	if t.pendingMemoryVersion != nil {
		v := *t.pendingMemoryVersion
		v.LedgerRevision = e.LedgerRevision
		payload, err := t.encode(v)
		if err != nil {
			return err
		}
		_, err = t.tx.Exec(ctx, `INSERT INTO memory_versions(tenant_id,id,record_id,scope_id,subtype,schema_version,ledger_revision,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, t.tenant, v.ID, v.RecordID, v.Proposal.Key.Scope, v.Proposal.Subtype, v.Proposal.SchemaVersion, e.LedgerRevision, payload)
		if err != nil {
			return err
		}
		seen := map[string]bool{}
		for _, span := range v.Proposal.Sources {
			if seen[span.EvidenceID] {
				continue
			}
			seen[span.EvidenceID] = true
			if _, err = t.tx.Exec(ctx, `INSERT INTO memory_evidence VALUES($1,$2,$3,$4)`, t.tenant, v.Proposal.Key.Scope, v.ID, span.EvidenceID); err != nil {
				return err
			}
		}
		for _, d := range v.Proposal.Dependencies {
			if _, err = t.tx.Exec(ctx, `INSERT INTO memory_dependencies VALUES($1,$2,$3,$4,$5)`, t.tenant, v.ID, d.Scope, d.RecordID, d.VersionID); err != nil {
				return err
			}
		}
	}
	return nil
}
