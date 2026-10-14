package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

// AuditEntries gathers an entire scope from the caller's consistent transaction
// view. Limits fail explicitly rather than silently truncate an export.
func (t *transaction) AuditEntries(ctx context.Context, scope string) ([]model.AuditEntry, []model.AuditGap, error) {
	entries := []model.AuditEntry{}
	gaps := []model.AuditGap{}
	totalBytes := 0
	appendValue := func(kind, id string, v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		totalBytes += len(b)
		entries = append(entries, model.AuditEntry{Kind: kind, ID: id, Payload: b})
		if len(entries) > 10000 || totalBytes > 64<<20 {
			return model.ErrPrecondition
		}
		return nil
	}
	queries := []struct{ kind, query string }{
		{"context", `SELECT c.id,c.payload FROM context_manifests c WHERE c.tenant_id=$1 AND EXISTS(SELECT 1 FROM context_scopes s WHERE s.tenant_id=c.tenant_id AND s.context_id=c.id AND s.scope_id=$2) ORDER BY c.id`},
		{"decision", `SELECT c.id,c.payload FROM decision_receipts c WHERE c.tenant_id=$1 AND EXISTS(SELECT 1 FROM decision_scopes s WHERE s.tenant_id=c.tenant_id AND s.decision_id=c.id AND s.scope_id=$2) ORDER BY c.id`},
		{"retention", `SELECT id,payload FROM retention_events WHERE tenant_id=$1 AND scope_id=$2 ORDER BY ledger_revision`},
		{"policy", `SELECT id,payload FROM policy_versions WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id`},
		{"predicate", `SELECT predicate||'/'||version,payload FROM predicate_versions WHERE tenant_id=$1 AND scope_id=$2 ORDER BY predicate,version`},
		{"version", `SELECT id,payload FROM record_versions WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id`},
		{"evidence", `SELECT id,payload FROM evidence WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id`},
		{"attestation", `SELECT id,payload FROM attestations WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id`},
		{"snapshot", `SELECT s.id,s.payload FROM fact_snapshots s WHERE s.tenant_id=$1 AND EXISTS(SELECT 1 FROM fact_snapshot_items i WHERE i.tenant_id=s.tenant_id AND i.snapshot_id=s.id AND i.scope_id=$2) ORDER BY s.id`},
		{"calculation", `SELECT c.id,c.payload FROM calculation_receipts c WHERE c.tenant_id=$1 AND (c.scope_id=$2 OR EXISTS(SELECT 1 FROM calculation_snapshots cs JOIN fact_snapshot_items i ON i.tenant_id=cs.tenant_id AND i.snapshot_id=cs.snapshot_id WHERE cs.tenant_id=c.tenant_id AND cs.calculation_id=c.id AND i.scope_id=$2)) ORDER BY c.id`},
	}
	for _, q := range queries {
		rows, err := t.tx.Query(ctx, q.query+" LIMIT 10001", t.tenant, scope)
		if err != nil {
			return nil, nil, err
		}
		for rows.Next() {
			var id string
			var encrypted []byte
			if err := rows.Scan(&id, &encrypted); err != nil {
				rows.Close()
				return nil, nil, err
			}
			plain, err := t.cipher.Open(t.tenant, encrypted)
			if err != nil {
				rows.Close()
				return nil, nil, err
			}
			totalBytes += len(plain)
			entries = append(entries, model.AuditEntry{Kind: q.kind, ID: id, Payload: plain})
			if len(entries) > 10000 || totalBytes > 64<<20 {
				rows.Close()
				return nil, nil, model.ErrPrecondition
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, nil, err
		}
	}
	rows, err := t.tx.Query(ctx, `SELECT id,subject_id,predicate,discriminator,stream_revision FROM records WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id LIMIT 10001`, t.tenant, scope)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		r := model.Record{Key: model.FactKey{Scope: scope}}
		if err := rows.Scan(&r.ID, &r.Key.Subject, &r.Key.Predicate, &r.Key.Discriminator, &r.Revision); err != nil {
			rows.Close()
			return nil, nil, err
		}
		if err := appendValue("record", r.ID, r); err != nil {
			rows.Close()
			return nil, nil, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	rows, err = t.tx.Query(ctx, `SELECT payload,recorded_at FROM ledger_events WHERE tenant_id=$1 AND scope_id=$2 ORDER BY ledger_revision LIMIT 10001`, t.tenant, scope)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var b []byte
		var at time.Time
		if err := rows.Scan(&b, &at); err != nil {
			rows.Close()
			return nil, nil, err
		}
		var e model.Event
		if err := t.decode(b, &e); err != nil {
			rows.Close()
			return nil, nil, err
		}
		e.RecordedAt = at
		if err := appendValue("event", fmt.Sprintf("%020d", e.LedgerRevision), e); err != nil {
			rows.Close()
			return nil, nil, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	rows, err = t.tx.Query(ctx, `SELECT h.record_id,h.ledger_revision,h.ordinal,h.payload FROM timeline_history h JOIN records r ON r.tenant_id=h.tenant_id AND r.id=h.record_id WHERE h.tenant_id=$1 AND r.scope_id=$2 ORDER BY h.record_id,h.ledger_revision,h.ordinal LIMIT 10001`, t.tenant, scope)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var record string
		var revision int64
		var ordinal int
		var b []byte
		if err := rows.Scan(&record, &revision, &ordinal, &b); err != nil {
			rows.Close()
			return nil, nil, err
		}
		var a model.Acceptance
		if err := t.decode(b, &a); err != nil {
			rows.Close()
			return nil, nil, err
		}
		value := struct {
			RecordID       string
			LedgerRevision int64
			Ordinal        int
			Interval       model.Acceptance
		}{record, revision, ordinal, a}
		if err := appendValue("timeline", fmt.Sprintf("%s/%020d/%06d", record, revision, ordinal), value); err != nil {
			rows.Close()
			return nil, nil, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	rows, err = t.tx.Query(ctx, `SELECT e.attestation_id,e.actor,e.recorded_at,e.ledger_revision FROM attestation_events e JOIN attestations a ON a.tenant_id=e.tenant_id AND a.id=e.attestation_id WHERE e.tenant_id=$1 AND a.scope_id=$2 ORDER BY e.attestation_id LIMIT 10001`, t.tenant, scope)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var value struct {
			AttestationID, Actor string
			RecordedAt           time.Time
			LedgerRevision       *int64
		}
		if err := rows.Scan(&value.AttestationID, &value.Actor, &value.RecordedAt, &value.LedgerRevision); err != nil {
			rows.Close()
			return nil, nil, err
		}
		if value.LedgerRevision == nil {
			gaps = append(gaps, model.AuditGap{Kind: "attestation_withdrawal", ID: value.AttestationID, Reason: "legacy_revision_unavailable"})
		}
		if err := appendValue("attestation_withdrawal", value.AttestationID, value); err != nil {
			rows.Close()
			return nil, nil, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	return entries, gaps, nil
}
