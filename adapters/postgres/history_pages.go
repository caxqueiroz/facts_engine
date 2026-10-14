package postgres

import (
	"context"
	"time"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) HistoryPage(ctx context.Context, record string, through, after int64, limit int) ([]model.Event, error) {
	return t.recordHistoryPage(ctx, record, through, after, limit, false)
}
func (t *transaction) MemoryHistoryPage(ctx context.Context, record string, through, after int64, limit int) ([]model.Event, error) {
	return t.recordHistoryPage(ctx, record, through, after, limit, true)
}
func (t *transaction) recordHistoryPage(ctx context.Context, record string, through, after int64, limit int, memory bool) ([]model.Event, error) {
	if through < 0 || after < 0 || limit < 1 || limit > 100 {
		return nil, model.ErrInvalid
	}
	query := ledgerdb.ListFactHistoryPage
	if memory {
		query = ledgerdb.ListMemoryHistoryPage
	}
	rows, err := t.tx.Query(ctx, query, t.tenant, record, after, through, limit)
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
