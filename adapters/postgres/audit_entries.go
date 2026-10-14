package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
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
		{"scope_identity", ledgerdb.ListAuditScopeIdentity},
		{"subject_identity", ledgerdb.ListAuditSubjectIdentity},
		{"memory_timeline", ledgerdb.ListAuditMemoryTimeline},
		{"memory_type", ledgerdb.ListAuditMemoryType},
		{"memory_version", ledgerdb.ListAuditMemoryVersion},
		{"context", ledgerdb.ListAuditContext},
		{"decision", ledgerdb.ListAuditDecision},
		{"extraction_attempt", ledgerdb.ListAuditExtractionAttempt},
		{"extraction_result", ledgerdb.ListAuditExtractionResult},
		{"extraction_retry", ledgerdb.ListAuditExtractionRetry},
		{"extraction_job", ledgerdb.ListAuditExtractionJob},
		{"conversation", ledgerdb.ListAuditConversation},
		{"retention", ledgerdb.ListAuditRetention},
		{"policy", ledgerdb.ListAuditPolicy},
		{"predicate", ledgerdb.ListAuditPredicate},
		{"version", ledgerdb.ListAuditVersion},
		{"evidence", ledgerdb.ListAuditEvidence},
		{"memory_attestation", ledgerdb.ListAuditMemoryAttestation},
		{"attestation", ledgerdb.ListAuditAttestation},
		{"snapshot", ledgerdb.ListAuditSnapshot},
		{"calculation", ledgerdb.ListAuditCalculation},
	}
	for _, q := range queries {
		rows, err := t.tx.Query(ctx, q.query, t.tenant, scope)
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
	rows, err := t.tx.Query(ctx, ledgerdb.ListAuditRecords, t.tenant, scope)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		r := model.Record{Key: model.FactKey{Scope: scope}}
		if err := rows.Scan(&r.ID, &r.Key.Subject, &r.Key.Predicate, &r.Key.Discriminator, &r.Revision, &r.Kind); err != nil {
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
	rows, err = t.tx.Query(ctx, ledgerdb.ListAuditEvents, t.tenant, scope)
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
	rows, err = t.tx.Query(ctx, ledgerdb.ListAuditFactTimeline, t.tenant, scope)
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
	rows, err = t.tx.Query(ctx, ledgerdb.ListAuditAttestationWithdrawals, t.tenant, scope)
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
