package postgres

import (
	"context"
	"math"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) MemoryTimeline(ctx context.Context, record string, revision int64) ([]model.Acceptance, error) {
	rows, err := t.tx.Query(ctx, ledgerdb.ListMemoryTimelinePayloads, t.tenant, record, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := []model.Acceptance{}
	for rows.Next() {
		var payload []byte
		if err = rows.Scan(&payload); err != nil {
			return nil, err
		}
		var entry model.Acceptance
		if err = t.decode(payload, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}
func (t *transaction) StageMemoryTimeline(_ context.Context, record string, entries []model.Acceptance) error {
	t.pendingMemoryRecord = record
	t.pendingMemoryTimeline = entries
	return nil
}
func (t *transaction) StageMemoryRejection(_ context.Context, record, version string) error {
	t.pendingMemoryRecord = record
	t.pendingMemoryRejection = version
	return nil
}
func (t *transaction) MemoryRejected(ctx context.Context, record, version string, revision int64) (bool, error) {
	yes, err := ledgerdb.New(t.tx).MemoryRejected(ctx, ledgerdb.MemoryRejectedParams{TenantID: t.tenant, RecordID: record, VersionID: version, Revision: revision})
	return yes, err
}
func (t *transaction) MemoryHasAcceptance(ctx context.Context, record, version string) (bool, error) {
	yes, err := ledgerdb.New(t.tx).MemoryHasAcceptance(ctx, ledgerdb.MemoryHasAcceptanceParams{TenantID: t.tenant, RecordID: record, VersionID: version})
	return yes, err
}
func (t *transaction) MemoryStreamRevisionAt(ctx context.Context, record string, revision int64) (int64, error) {
	stream, err := ledgerdb.New(t.tx).GetMemoryStreamRevisionAt(ctx, ledgerdb.GetMemoryStreamRevisionAtParams{TenantID: t.tenant, RecordID: record, LedgerRevision: revision})
	return stream, err
}
func (t *transaction) flushMemoryLifecycle(ctx context.Context, e model.Event) error {
	if err := t.flushMemoryAttestations(ctx, e); err != nil {
		return err
	}
	if t.pendingMemoryTimeline != nil {
		if err := ledgerdb.New(t.tx).InsertMemoryTimelineGeneration(ctx, ledgerdb.InsertMemoryTimelineGenerationParams{TenantID: t.tenant, RecordID: t.pendingMemoryRecord, LedgerRevision: e.LedgerRevision}); err != nil {
			return err
		}
		for ordinal, entry := range t.pendingMemoryTimeline {
			if ordinal > math.MaxInt32 {
				return model.ErrInvalid
			}
			if entry.LedgerRevision == 0 {
				entry.LedgerRevision = e.LedgerRevision
			}
			payload, err := t.encode(entry)
			if err != nil {
				return err
			}
			if err = ledgerdb.New(t.tx).InsertMemoryTimelineEntry(ctx, ledgerdb.InsertMemoryTimelineEntryParams{TenantID: t.tenant, RecordID: t.pendingMemoryRecord, LedgerRevision: e.LedgerRevision, Ordinal: int32(ordinal), VersionID: entry.VersionID, ValidFrom: nullableTime(entry.Valid.From), ValidTo: nullableTime(entry.Valid.To), Payload: payload}); err != nil {
				return err
			}
		}
	}
	if t.pendingMemoryRejection != "" {
		if err := ledgerdb.New(t.tx).InsertMemoryRejection(ctx, ledgerdb.InsertMemoryRejectionParams{TenantID: t.tenant, RecordID: t.pendingMemoryRecord, VersionID: t.pendingMemoryRejection, LedgerRevision: e.LedgerRevision}); err != nil {
			return err
		}
	}
	return nil
}
