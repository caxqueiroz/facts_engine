package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

func (t *transaction) RevisionAt(ctx context.Context, knownAt time.Time) (int64, error) {
	revision, err := ledgerdb.New(t.tx).GetRevisionAt(ctx, ledgerdb.GetRevisionAtParams{TenantID: t.tenant, RecordedAt: nullableTime(&knownAt)})
	return revision, err
}
func (t *transaction) FactRecords(ctx context.Context, q model.FactsQuery, revision int64, after string, limit int) ([]model.Record, error) {
	rows, err := t.tx.Query(ctx, ledgerdb.ListFactRecords, t.tenant, q.Scope, q.Subject, q.Predicate, after, revision, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Record{}
	for rows.Next() {
		var r model.Record
		if err = rows.Scan(&r.ID, &r.Key.Scope, &r.Key.Subject, &r.Key.Predicate, &r.Key.Discriminator); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
