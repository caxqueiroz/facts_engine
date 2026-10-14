package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

func (t *transaction) RevisionAt(ctx context.Context, knownAt time.Time) (int64, error) {
	var revision int64
	err := t.tx.QueryRow(ctx, `SELECT coalesce(max(ledger_revision),0) FROM ledger_events WHERE tenant_id=$1 AND recorded_at<=$2`, t.tenant, knownAt).Scan(&revision)
	return revision, err
}
func (t *transaction) FactRecords(ctx context.Context, q model.FactsQuery, revision int64, after string, limit int) ([]model.Record, error) {
	rows, err := t.tx.Query(ctx, `SELECT r.id,r.scope_id,r.subject_id,r.predicate,r.discriminator FROM records r WHERE r.kind='fact' AND r.tenant_id=$1 AND r.scope_id=$2 AND ($3='' OR r.subject_id=$3) AND ($4='' OR r.predicate=$4) AND r.id>$5 AND EXISTS(SELECT 1 FROM record_events re JOIN ledger_events e ON e.tenant_id=re.tenant_id AND e.id=re.event_id WHERE re.tenant_id=r.tenant_id AND re.record_id=r.id AND e.ledger_revision<=$6) ORDER BY r.id LIMIT $7`, t.tenant, q.Scope, q.Subject, q.Predicate, after, revision, limit)
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
