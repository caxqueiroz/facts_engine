package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) MemoryRefreshEvent(ctx context.Context, q model.MemoryRefreshTrigger) (model.Event, error) {
	var e model.Event
	err := t.read(ctx, &e, `SELECT payload FROM ledger_events WHERE tenant_id=$1 AND id=$2 AND ledger_revision=$3`, t.tenant, q.EventID, q.LedgerRevision)
	return e, err
}
func (t *transaction) MemoryRefreshDependents(ctx context.Context, scope, record string, revision int64, after string, limit int) ([]model.VersionReference, error) {
	rows, err := t.tx.Query(ctx, `SELECT DISTINCT v.record_id,v.id FROM memory_versions v JOIN memory_dependencies d ON d.tenant_id=v.tenant_id AND d.memory_version_id=v.id WHERE v.tenant_id=$1 AND v.scope_id=$2 AND d.record_id=$3 AND v.ledger_revision<=$4 AND v.id>$5 AND EXISTS(SELECT 1 FROM memory_timeline_history h WHERE h.tenant_id=v.tenant_id AND h.record_id=v.record_id AND h.version_id=v.id AND h.ledger_revision=(SELECT max(g.ledger_revision) FROM memory_timeline_generations g WHERE g.tenant_id=v.tenant_id AND g.record_id=v.record_id AND g.ledger_revision<=$4)) ORDER BY v.id,v.record_id LIMIT $6`, t.tenant, scope, record, revision, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.VersionReference{}
	for rows.Next() {
		var v model.VersionReference
		if err = rows.Scan(&v.RecordID, &v.VersionID); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
