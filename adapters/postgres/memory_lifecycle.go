package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) MemoryTimeline(ctx context.Context, record string, revision int64) ([]model.Acceptance, error) {
	rows, err := t.tx.Query(ctx, `SELECT payload FROM memory_timeline_history WHERE tenant_id=$1 AND record_id=$2 AND ledger_revision=(SELECT max(ledger_revision) FROM memory_timeline_generations WHERE tenant_id=$1 AND record_id=$2 AND ($3::bigint=0 OR ledger_revision<=$3)) ORDER BY ordinal`, t.tenant, record, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := []model.Acceptance{}
	for rows.Next() {
		var payload []byte
		if err = rows.Scan(&payload); err != nil {
			return nil, err
		}
		var entry model.Acceptance
		if err = t.decode(payload, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}
func (t *transaction) StageMemoryTimeline(_ context.Context, record string, entries []model.Acceptance) error {
	t.pendingMemoryRecord = record
	t.pendingMemoryTimeline = entries
	return nil
}
func (t *transaction) StageMemoryRejection(_ context.Context, record, version string) error {
	t.pendingMemoryRecord = record
	t.pendingMemoryRejection = version
	return nil
}
func (t *transaction) MemoryRejected(ctx context.Context, record, version string, revision int64) (bool, error) {
	var yes bool
	err := t.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM memory_rejections WHERE tenant_id=$1 AND record_id=$2 AND version_id=$3 AND ($4::bigint=0 OR ledger_revision<=$4))`, t.tenant, record, version, revision).Scan(&yes)
	return yes, err
}
func (t *transaction) MemoryHasAcceptance(ctx context.Context, record, version string) (bool, error) {
	var yes bool
	err := t.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM memory_timeline_history WHERE tenant_id=$1 AND record_id=$2 AND version_id=$3)`, t.tenant, record, version).Scan(&yes)
	return yes, err
}
func (t *transaction) MemoryStreamRevisionAt(ctx context.Context, record string, revision int64) (int64, error) {
	var stream int64
	err := t.tx.QueryRow(ctx, `SELECT coalesce(max(r.stream_revision),0) FROM memory_record_events r JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id WHERE r.tenant_id=$1 AND r.record_id=$2 AND e.ledger_revision<=$3`, t.tenant, record, revision).Scan(&stream)
	return stream, err
}
func (t *transaction) flushMemoryLifecycle(ctx context.Context, e model.Event) error {
	if t.pendingMemoryTimeline != nil {
		if _, err := t.tx.Exec(ctx, `INSERT INTO memory_timeline_generations VALUES($1,$2,$3)`, t.tenant, t.pendingMemoryRecord, e.LedgerRevision); err != nil {
			return err
		}
		for ordinal, entry := range t.pendingMemoryTimeline {
			if entry.LedgerRevision == 0 {
				entry.LedgerRevision = e.LedgerRevision
			}
			payload, err := t.encode(entry)
			if err != nil {
				return err
			}
			if _, err = t.tx.Exec(ctx, `INSERT INTO memory_timeline_history VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, t.tenant, t.pendingMemoryRecord, e.LedgerRevision, ordinal, entry.VersionID, entry.Valid.From, entry.Valid.To, payload); err != nil {
				return err
			}
		}
	}
	if t.pendingMemoryRejection != "" {
		if _, err := t.tx.Exec(ctx, `INSERT INTO memory_rejections VALUES($1,$2,$3,$4)`, t.tenant, t.pendingMemoryRecord, t.pendingMemoryRejection, e.LedgerRevision); err != nil {
			return err
		}
	}
	return nil
}
