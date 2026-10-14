package postgres

import (
	"context"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) HistoryPage(ctx context.Context, record string, through, after int64, limit int) ([]model.Event, error) {
	if through < 0 || after < 0 || limit < 1 || limit > 100 {
		return nil, model.ErrInvalid
	}
	rows, err := t.tx.Query(ctx, `SELECT e.payload,e.recorded_at FROM record_events r JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id WHERE r.tenant_id=$1 AND r.record_id=$2 AND r.stream_revision>$3 AND e.ledger_revision<=$4 ORDER BY r.stream_revision LIMIT $5`, t.tenant, record, after, through, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []model.Event{}
	for rows.Next() {
		var payload []byte
		var at time.Time
		if err = rows.Scan(&payload, &at); err != nil {
			return nil, err
		}
		var event model.Event
		if err = t.decode(payload, &event); err != nil {
			return nil, err
		}
		event.RecordedAt = at
		events = append(events, event)
	}
	return events, rows.Err()
}
