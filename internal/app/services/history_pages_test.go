package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

type historyPageLedger struct {
	model.Ledger
	tx model.Tx
}

func (l historyPageLedger) Transact(_ context.Context, _ string, fn func(model.Tx) error) error {
	return fn(l.tx)
}

type historyPageTx struct {
	model.Tx
	gap bool
}

func (t historyPageTx) Record(context.Context, string) (model.Record, error) {
	return model.Record{ID: "record", Key: model.FactKey{Scope: "scope"}, Revision: 1002}, nil
}
func (t historyPageTx) Revision(context.Context) (int64, error) { return 2000, nil }
func (t historyPageTx) HistoryPage(_ context.Context, _ string, _ int64, after int64, limit int) ([]model.Event, error) {
	events := []model.Event{}
	for revision := after + 1; revision <= 1002 && len(events) < limit; revision++ {
		if t.gap && revision == 500 {
			continue
		}
		events = append(events, model.Event{RecordID: "record", Scope: "scope", StreamRevision: revision, LedgerRevision: revision})
	}
	return events, nil
}
func TestHistoryPaginationBeyondThousandAndGapDetection(t *testing.T) {
	for _, gap := range []bool{false, true} {
		s := &Service{ledger: historyPageLedger{tx: historyPageTx{gap: gap}}, auth: waitAuth{check: func() bool { return true }}, reads: model.ReadOptions{PaginationKey: make([]byte, 32), TokenLifetime: time.Minute}}
		q := model.HistoryPageQuery{HistoryQuery: model.HistoryQuery{Scope: "scope", RecordID: "record", Purpose: "audit"}, Limit: 100}
		caller := model.Caller{Tenant: "tenant", Principal: "reader"}
		count := 0
		var finalErr error
		for i := 0; i < 20; i++ {
			page, err := s.ListFactHistory(context.Background(), caller, q)
			if err != nil {
				finalErr = err
				break
			}
			for _, event := range page.Events {
				count++
				if event.StreamRevision != int64(count) {
					t.Fatal("skipped event", event)
				}
			}
			q.PageToken = page.NextPageToken
			if q.PageToken == "" {
				break
			}
		}
		if gap {
			if !errors.Is(finalErr, model.ErrPrecondition) {
				t.Fatal("gap hidden", finalErr)
			}
		} else if finalErr != nil || count != 1002 {
			t.Fatal(count, finalErr)
		}
	}
}
