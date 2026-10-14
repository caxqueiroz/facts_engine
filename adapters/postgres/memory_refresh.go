package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) MemoryRefreshEvent(ctx context.Context, q model.MemoryRefreshTrigger) (model.Event, error) {
	var e model.Event
	payload, err := ledgerdb.New(t.tx).GetEventAtRevision(ctx, ledgerdb.GetEventAtRevisionParams{TenantID: t.tenant, ID: q.EventID, LedgerRevision: q.LedgerRevision})
	if err != nil {
		return e, translate(err)
	}
	err = t.decode(payload, &e)
	return e, err
}
func (t *transaction) MemoryRefreshDependents(ctx context.Context, scope, record string, revision int64, after string, limit int) ([]model.VersionReference, error) {
	rows, err := t.tx.Query(ctx, ledgerdb.ListMemoryRefreshDependents, t.tenant, scope, record, revision, after, limit)
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
