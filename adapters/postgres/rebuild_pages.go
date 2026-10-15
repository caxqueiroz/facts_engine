package postgres

import (
	"context"
	"encoding/json"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (l *Ledger) ProjectionDocumentPage(ctx context.Context, k model.GenerationKey, fence, through int64, after model.ProjectionCursor, limit int) (model.ProjectionPage, error) {
	empty := model.ProjectionPage{}
	if !validGeneration(k) || fence < 1 || through < 0 || limit < 1 || limit > model.MaxProjectionPageDocuments || (after != (model.ProjectionCursor{}) && !after.Valid()) {
		return empty, model.ErrInvalid
	}
	events := []model.Event{}
	err := l.Transact(ctx, k.Tenant, func(tx model.Tx) error {
		events = nil
		t := tx.(*transaction)
		status, err := t.generation(ctx, k)
		if err != nil {
			return err
		}
		if status.Fence != fence || status.ThroughRevision != through {
			return model.ErrRevision
		}
		if status.State != "paused" && status.State != "verified" {
			return model.ErrPrecondition
		}
		rows, err := t.tx.Query(ctx, ledgerdb.ListProjectionEventPage, k.Tenant, through, after.Kind, after.VersionID, limit+1)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var payload []byte
			if err := rows.Scan(&payload); err != nil {
				return err
			}
			var event model.Event
			if err := t.decode(payload, &event); err != nil {
				return err
			}
			events = append(events, event)
		}
		return rows.Err()
	})
	if err != nil {
		return empty, err
	}
	page := model.ProjectionPage{Documents: []model.ProjectionDocument{}}
	size, scanned := 0, 0
	last := after
	for _, event := range events {
		// Bound scanned events as well as returned documents, so disposed-only
		// pages advance without an unbounded search for the next retained payload.
		if scanned == limit {
			break
		}
		document, err := l.ProjectionDocument(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: k.Tenant}, Event: event}, k.Generation)
		if err != nil {
			return empty, err
		}
		if document != nil {
			payload, err := json.Marshal(document)
			if err != nil {
				return empty, err
			}
			if len(payload) > model.MaxProjectionPageBytes {
				return empty, model.ErrPrecondition
			}
			if size+len(payload) > model.MaxProjectionPageBytes {
				break
			}
			size += len(payload)
			page.Documents = append(page.Documents, *document)
		}
		kind := event.RecordKind
		if kind == "" {
			kind = "fact"
		}
		next := model.ProjectionCursor{Kind: kind, VersionID: event.VersionID}
		if !next.Valid() || !last.Before(next) {
			return empty, model.ErrPrecondition
		}
		last = next
		scanned++
	}
	if scanned < len(events) {
		if scanned == 0 {
			return empty, model.ErrPrecondition
		}
		page.Next = &last
	}

	return page, nil
}

var _ model.PagedRebuildStore = (*Ledger)(nil)
