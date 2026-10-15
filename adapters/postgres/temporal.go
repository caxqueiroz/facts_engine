package postgres

import (
	"context"
	"math"
	"time"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgtype"
)

func (t *transaction) StageTimeline(_ context.Context, record string, entries []model.Acceptance) error {
	if t.pendingRecord != "" && t.pendingRecord != record {
		return model.ErrInvalid
	}
	t.pendingRecord = record
	t.pendingTimeline = entries
	return nil
}
func (t *transaction) StageRejection(_ context.Context, record, version string) error {
	t.pendingRecord = record
	t.pendingRejection = version
	return nil
}
func (t *transaction) StageSnapshot(_ context.Context, s model.FactSnapshot) error {
	t.pendingSnapshot = &s
	return nil
}
func (t *transaction) flushTemporal(ctx context.Context, e model.Event) error {
	if t.pendingTimeline != nil {
		err := ledgerdb.New(t.tx).InsertTimelineGeneration(ctx, ledgerdb.InsertTimelineGenerationParams{TenantID: t.tenant, RecordID: t.pendingRecord, LedgerRevision: e.LedgerRevision})
		if err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).DeleteCurrentTimeline(ctx, ledgerdb.DeleteCurrentTimelineParams{TenantID: t.tenant, RecordID: t.pendingRecord}); err != nil {
			return err
		}
		for i, a := range t.pendingTimeline {
			if i > math.MaxInt32 {
				return model.ErrInvalid
			}
			if a.LedgerRevision == 0 {
				a.LedgerRevision = e.LedgerRevision
			}
			b, err := t.encode(a)
			if err != nil {
				return err
			}
			if err = ledgerdb.New(t.tx).InsertTimelineEntry(ctx, ledgerdb.InsertTimelineEntryParams{TenantID: t.tenant, RecordID: t.pendingRecord, LedgerRevision: e.LedgerRevision, Ordinal: int32(i), VersionID: a.VersionID, Payload: b}); err != nil {
				return err
			}
			if err = ledgerdb.New(t.tx).InsertAcceptedInterval(ctx, ledgerdb.InsertAcceptedIntervalParams{TenantID: t.tenant, RecordID: t.pendingRecord, VersionID: a.VersionID, ValidFrom: nullableTime(a.Valid.From), ValidTo: nullableTime(a.Valid.To), LedgerRevision: pgtype.Int8{Int64: e.LedgerRevision, Valid: true}, Payload: b}); err != nil {
				return err
			}
		}
	}
	if t.pendingRejection != "" {
		if err := ledgerdb.New(t.tx).InsertFactRejection(ctx, ledgerdb.InsertFactRejectionParams{TenantID: t.tenant, RecordID: t.pendingRecord, VersionID: t.pendingRejection, LedgerRevision: e.LedgerRevision}); err != nil {
			return err
		}
	}
	if t.pendingAttestation != nil {
		a := *t.pendingAttestation
		at := e.RecordedAt
		a.RecordedAt = &at
		b, err := t.encode(a)
		if err != nil {
			return err
		}
		if err := ledgerdb.New(t.tx).InsertFactAttestation(ctx, ledgerdb.InsertFactAttestationParams{TenantID: t.tenant, ID: a.ID, ScopeID: a.Scope, RecordID: a.RecordID, VersionID: a.VersionID, EvidenceID: a.EvidenceID, LedgerRevision: pgtype.Int8{Int64: e.LedgerRevision, Valid: true}}); err != nil {
			return err
		}
		if err := ledgerdb.New(t.tx).InsertFactAttestationPayload(ctx, ledgerdb.InsertFactAttestationPayloadParams{TenantID: t.tenant, AttestationID: a.ID, Payload: b}); err != nil {
			return err
		}
	}
	if t.pendingWithdrawal != "" {
		if err := ledgerdb.New(t.tx).InsertFactWithdrawal(ctx, ledgerdb.InsertFactWithdrawalParams{TenantID: t.tenant, AttestationID: t.pendingWithdrawal, Actor: e.Caller.Principal, LedgerRevision: pgtype.Int8{Int64: e.LedgerRevision, Valid: true}}); err != nil {
			return err
		}
	}

	if t.pendingSnapshot != nil {
		s := *t.pendingSnapshot
		s.CreationRevision = e.LedgerRevision
		b, err := t.encode(s)
		if err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertFactSnapshot(ctx, ledgerdb.InsertFactSnapshotParams{TenantID: t.tenant, ID: s.ID, SelectionRevision: s.SelectionRevision, CreationRevision: s.CreationRevision, Digest: s.Digest}); err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertFactSnapshotPayload(ctx, ledgerdb.InsertFactSnapshotPayloadParams{TenantID: t.tenant, SnapshotID: s.ID, Payload: b}); err != nil {
			return err
		}
		for i, item := range s.Items {
			if i > math.MaxInt32 {
				return model.ErrInvalid
			}
			var record, version *string
			if item.Resolution.RecordID != "" {
				v := item.Resolution.RecordID
				record = &v
			}
			if item.Resolution.VersionID != "" {
				v := item.Resolution.VersionID
				version = &v
			}
			if err = ledgerdb.New(t.tx).InsertFactSnapshotItem(ctx, ledgerdb.InsertFactSnapshotItemParams{TenantID: t.tenant, SnapshotID: s.ID, Ordinal: int32(i), ScopeID: item.Selector.Scope, SelectorDigest: model.Digest(item.Selector), RecordID: nullableText(record), VersionID: nullableText(version)}); err != nil {
				return err
			}
		}
	}
	return nil
}
func (t *transaction) Acceptances(ctx context.Context, record string, revision int64) ([]model.Acceptance, error) {
	query := ledgerdb.ListCurrentAcceptances
	args := []any{t.tenant, record}
	if revision > 0 {
		query = ledgerdb.ListHistoricalAcceptances
		args = append(args, revision)
	}
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Acceptance{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		var a model.Acceptance
		if err := t.decode(b, &a); err != nil {
			return nil, err
		}
		if a.State == "" {
			a.State = "accepted"
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (t *transaction) Rejected(ctx context.Context, record, version string, revision int64) (bool, error) {
	yes, err := ledgerdb.New(t.tx).FactRejected(ctx, ledgerdb.FactRejectedParams{TenantID: t.tenant, RecordID: record, VersionID: version, Revision: revision})
	return yes, err
}
func (t *transaction) HasAcceptance(ctx context.Context, record, version string) (bool, error) {
	yes, err := ledgerdb.New(t.tx).FactHasAcceptance(ctx, ledgerdb.FactHasAcceptanceParams{TenantID: t.tenant, RecordID: record, VersionID: version})
	return yes, err
}
func (t *transaction) Versions(ctx context.Context, record string, revision int64) ([]model.Version, error) {
	rows, err := t.tx.Query(ctx, ledgerdb.ListFactVersionPayloads, t.tenant, record, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Version{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		if len(b) == 0 {
			return nil, model.ErrPrecondition
		}
		var v model.Version
		if err := t.decode(b, &v); err != nil {
			return nil, err
		}
		if v.PayloadDisposed {
			return nil, model.ErrPrecondition
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (t *transaction) HistoricalAttestations(ctx context.Context, version string, revision int64) ([]model.Attestation, error) {
	// Fail explicitly when upgrading a ledger that cannot place old attestations in revision order.
	gap, err := ledgerdb.New(t.tx).HasUnorderedAttestations(ctx, ledgerdb.HasUnorderedAttestationsParams{TenantID: t.tenant, VersionID: version})
	if err != nil {
		return nil, err
	}
	if gap {
		return nil, model.ErrPrecondition
	}
	rows, err := t.tx.Query(ctx, ledgerdb.ListHistoricalAttestationPayloads, t.tenant, version, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Attestation{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		if len(b) == 0 {
			return nil, model.ErrPrecondition
		}
		var a model.Attestation
		if err := t.decode(b, &a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (t *transaction) RevisionTime(ctx context.Context, rev int64) (time.Time, error) {
	at, err := ledgerdb.New(t.tx).GetRevisionTime(ctx, ledgerdb.GetRevisionTimeParams{TenantID: t.tenant, LedgerRevision: rev})
	if err != nil {
		return time.Time{}, translate(err)
	}
	if !at.Valid || at.InfinityModifier != pgtype.Finite {
		return time.Time{}, model.ErrPrecondition
	}
	return at.Time, nil
}
func (t *transaction) StreamRevisionAt(ctx context.Context, record string, revision int64) (int64, error) {
	rev, err := ledgerdb.New(t.tx).GetFactStreamRevisionAt(ctx, ledgerdb.GetFactStreamRevisionAtParams{TenantID: t.tenant, RecordID: record, LedgerRevision: revision})
	return rev, err
}
func (t *transaction) Snapshot(ctx context.Context, id string) (model.FactSnapshot, error) {
	var s model.FactSnapshot
	payload, err := ledgerdb.New(t.tx).GetSnapshotPayload(ctx, ledgerdb.GetSnapshotPayloadParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return s, translate(err)
	}
	if len(payload) == 0 {
		return s, model.ErrPrecondition
	}
	err = t.decode(payload, &s)
	return s, err
}
func (t *transaction) History(ctx context.Context, record string) ([]model.Event, error) {
	rows, err := t.tx.Query(ctx, ledgerdb.ListFactHistoryPayloads, t.tenant, record)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Event{}
	for rows.Next() {
		var b []byte
		var at time.Time
		if err := rows.Scan(&b, &at); err != nil {
			return nil, err
		}
		var e model.Event
		if err := t.decode(b, &e); err != nil {
			return nil, err
		}
		e.RecordedAt = at
		out = append(out, e)
	}
	if len(out) > 1000 {
		return nil, model.ErrPrecondition
	}
	return out, rows.Err()
}

var _ model.TemporalTx = (*transaction)(nil)
