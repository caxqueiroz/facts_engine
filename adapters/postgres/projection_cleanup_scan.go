package postgres

import (
	"context"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (l *Ledger) ScanProjectionCleanup(ctx context.Context, r model.ProjectionCleanupQuery) ([]model.ProjectionCleanupRequest, error) {
	if !model.ValidID(r.Tenant) || !r.After.Valid() || r.Limit < 1 || r.Limit > 100 {
		return nil, model.ErrInvalid
	}
	var out []model.ProjectionCleanupRequest
	err := l.Transact(ctx, r.Tenant, func(tx model.Tx) error {
		out = []model.ProjectionCleanupRequest{}
		t := tx.(*transaction)
		rows, err := ledgerdb.New(t.tx).ScanProjectionCleanup(ctx, ledgerdb.ScanProjectionCleanupParams{TenantID: r.Tenant, AfterKind: r.After.Kind, AfterVersion: r.After.VersionID, AfterGeneration: r.After.Generation, PageLimit: int32(r.Limit)})
		if err != nil {
			return err
		}
		cursor := r.After
		size := 0
		for _, row := range rows {
			next := model.ProjectionCleanupCursor{Kind: row.Kind, VersionID: row.VersionID, Generation: row.Generation}
			size += len(row.Payload)
			if !next.Valid() || next.Compare(cursor) <= 0 || !model.ValidID(row.EventID) || !model.ValidID(row.ScopeID) || row.LedgerRevision < 1 || len(row.Payload) == 0 || size > 64<<20 {
				return model.ErrPrecondition
			}
			var event model.RetentionEvent
			if err = t.decode(row.Payload, &event); err != nil {
				return err
			}
			v := event.VersionDisposal
			if v == nil || event.ID != row.EventID || event.Tenant != r.Tenant || event.Caller.Tenant != r.Tenant || event.Kind != "dispose_"+row.Kind+"_payload" || event.TargetID != row.VersionID || event.Scope != row.ScopeID || event.LedgerRevision != row.LedgerRevision || v.Kind != row.Kind || v.VersionID != row.VersionID || v.Scope != row.ScopeID || !model.ValidID(v.RecordID) || v.Purpose == "" || len(v.Purpose) > 256 || v.Purpose != event.Command.Purpose || len(v.Bindings) < 1 || len(v.Bindings) > 256 {
				return model.ErrPrecondition
			}
			out = append(out, model.ProjectionCleanupRequest{Command: model.Command{Purpose: v.Purpose}, Kind: row.Kind, VersionID: row.VersionID, Generation: row.Generation, DisposalEventID: row.EventID, Scope: row.ScopeID, RecordID: v.RecordID, Bindings: append([]model.VersionRetentionBinding(nil), v.Bindings...)})
			cursor = next
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _ model.ProjectionCleanupScanner = (*Ledger)(nil)
