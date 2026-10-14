package postgres

import (
	"context"
	"errors"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (t *transaction) MemoryType(ctx context.Context, scope, subtype, version string) (model.MemoryType, error) {
	var d model.MemoryType
	payload, err := ledgerdb.New(t.tx).GetMemoryTypePayload(ctx, ledgerdb.GetMemoryTypePayloadParams{TenantID: t.tenant, ScopeID: scope, Subtype: subtype, Version: version})
	if err != nil {
		return d, translate(err)
	}
	err = t.decode(payload, &d)
	return d, err
}
func (t *transaction) ActiveMemoryType(ctx context.Context, scope, subtype string) (string, error) {
	version, err := ledgerdb.New(t.tx).GetActiveMemoryType(ctx, ledgerdb.GetActiveMemoryTypeParams{TenantID: t.tenant, ScopeID: scope, Subtype: subtype})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", model.ErrNotFound
	}
	return version, err
}
func (t *transaction) StageMemoryType(ctx context.Context, d model.MemoryType) error {
	// Lock/update the active pointer before taking the tenant append lock. Its
	// deferred FK is satisfied by the immutable definition flushed with the event.
	err := ledgerdb.New(t.tx).SetActiveMemoryType(ctx, ledgerdb.SetActiveMemoryTypeParams{TenantID: t.tenant, ScopeID: d.Scope, Subtype: d.Subtype, Version: d.Version})
	if err != nil {
		return err
	}
	t.pendingMemoryType = &d
	return nil
}
func (t *transaction) LockMemory(ctx context.Context, k model.MemoryKey) (model.Record, error) {
	id := model.MemoryRecordID(k)
	err := ledgerdb.New(t.tx).EnsureMemoryRecord(ctx, ledgerdb.EnsureMemoryRecordParams{TenantID: t.tenant, ID: id, ScopeID: k.Scope, Discriminator: k.ID})
	if err != nil {
		return model.Record{}, err
	}
	var r model.Record
	locked, err := ledgerdb.New(t.tx).LockMemoryRecord(ctx, ledgerdb.LockMemoryRecordParams{TenantID: t.tenant, ID: id})
	r.Revision, r.Kind = locked.StreamRevision, locked.Kind
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
	payload, err := ledgerdb.New(t.tx).GetMemoryVersionPayload(ctx, ledgerdb.GetMemoryVersionPayloadParams{TenantID: t.tenant, RecordID: record, ID: version})
	if err != nil {
		return v, translate(err)
	}
	err = t.decode(payload, &v)
	return v, err
}
func (t *transaction) flushMemory(ctx context.Context, e model.Event) error {
	if t.pendingMemoryType != nil {
		d := *t.pendingMemoryType
		payload, err := t.encode(d)
		if err != nil {
			return err
		}
		var from pgtype.Text
		if d.Migration != nil {
			from = pgtype.Text{String: d.Migration.FromVersion, Valid: true}
		}
		err = ledgerdb.New(t.tx).InsertMemoryTypePayload(ctx, ledgerdb.InsertMemoryTypePayloadParams{TenantID: t.tenant, ScopeID: d.Scope, Subtype: d.Subtype, Version: d.Version, PolicyID: d.PolicyID, FromVersion: from, LedgerRevision: e.LedgerRevision, Payload: payload})
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
		err = ledgerdb.New(t.tx).InsertMemoryVersionPayload(ctx, ledgerdb.InsertMemoryVersionPayloadParams{TenantID: t.tenant, ID: v.ID, RecordID: v.RecordID, ScopeID: v.Proposal.Key.Scope, Subtype: v.Proposal.Subtype, SchemaVersion: v.Proposal.SchemaVersion, LedgerRevision: e.LedgerRevision, Payload: payload})
		if err != nil {
			return err
		}
		seen := map[string]bool{}
		for _, span := range v.Proposal.Sources {
			if seen[span.EvidenceID] {
				continue
			}
			seen[span.EvidenceID] = true
			if err = ledgerdb.New(t.tx).InsertMemoryEvidence(ctx, ledgerdb.InsertMemoryEvidenceParams{TenantID: t.tenant, ScopeID: v.Proposal.Key.Scope, VersionID: v.ID, EvidenceID: span.EvidenceID}); err != nil {
				return err
			}
		}
		for _, d := range v.Proposal.Dependencies {
			if err = ledgerdb.New(t.tx).InsertMemoryDependency(ctx, ledgerdb.InsertMemoryDependencyParams{TenantID: t.tenant, MemoryVersionID: v.ID, ScopeID: d.Scope, RecordID: d.RecordID, VersionID: d.VersionID}); err != nil {
				return err
			}
		}
	}
	t.pendingMemoryVersion = nil
	t.pendingMemoryType = nil
	return nil
}

func (t *transaction) MemoryVersions(ctx context.Context, record string, revision int64) ([]model.MemoryVersion, error) {
	// Stream the generated query so count/byte limits apply before all payloads are collected.
	rows, err := t.tx.Query(ctx, ledgerdb.ListMemoryVersionPayloads, t.tenant, record, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.MemoryVersion{}
	size := 0
	for rows.Next() {
		var payload []byte
		if err = rows.Scan(&payload); err != nil {
			return nil, err
		}
		size += len(payload)
		if len(out) >= 10000 || size > 64<<20 {
			return nil, model.ErrPrecondition
		}
		var version model.MemoryVersion
		if err = t.decode(payload, &version); err != nil {
			return nil, err
		}
		out = append(out, version)
	}
	return out, rows.Err()
}
