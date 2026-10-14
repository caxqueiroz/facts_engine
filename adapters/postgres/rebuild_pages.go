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
	size := 0
	for _, event := range events {
		if len(page.Documents) == limit {
			break
		}
		document, err := l.ProjectionDocument(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: k.Tenant}, Event: event}, k.Generation)
		if err != nil {
			return empty, err
		}
		if document == nil {
			return empty, model.ErrPrecondition
		}
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
	if len(page.Documents) < len(events) {
		if len(page.Documents) == 0 {
			return empty, model.ErrPrecondition
		}
		next := model.ProjectionDocumentCursor(page.Documents[len(page.Documents)-1])
		page.Next = &next
	}
	return page, nil
}

var _ model.PagedRebuildStore = (*Ledger)(nil)
