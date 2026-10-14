package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
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
		_, err := t.tx.Exec(ctx, `INSERT INTO timeline_generations VALUES($1,$2,$3)`, t.tenant, t.pendingRecord, e.LedgerRevision)
		if err != nil {
			return err
		}
		if _, err = t.tx.Exec(ctx, `DELETE FROM accepted_intervals WHERE tenant_id=$1 AND record_id=$2`, t.tenant, t.pendingRecord); err != nil {
			return err
		}
		for i, a := range t.pendingTimeline {
			if a.LedgerRevision == 0 {
				a.LedgerRevision = e.LedgerRevision
			}
			b, err := t.encode(a)
			if err != nil {
				return err
			}
			if _, err = t.tx.Exec(ctx, `INSERT INTO timeline_history VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, t.pendingRecord, e.LedgerRevision, i, a.VersionID, b); err != nil {
				return err
			}
			if _, err = t.tx.Exec(ctx, `INSERT INTO accepted_intervals(tenant_id,record_id,version_id,valid_from,valid_to,ledger_revision,payload) VALUES($1,$2,$3,$4,$5,$6,$7)`, t.tenant, t.pendingRecord, a.VersionID, a.Valid.From, a.Valid.To, e.LedgerRevision, b); err != nil {
				return err
			}
		}
	}
	if t.pendingRejection != "" {
		if _, err := t.tx.Exec(ctx, `INSERT INTO rejected_versions VALUES($1,$2,$3,$4)`, t.tenant, t.pendingRecord, t.pendingRejection, e.LedgerRevision); err != nil {
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
		if _, err := t.tx.Exec(ctx, `INSERT INTO attestations(tenant_id,id,scope_id,record_id,version_id,evidence_id,payload,ledger_revision) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, t.tenant, a.ID, a.Scope, a.RecordID, a.VersionID, a.EvidenceID, b, e.LedgerRevision); err != nil {
			return err
		}
	}
	if t.pendingWithdrawal != "" {
		if _, err := t.tx.Exec(ctx, `INSERT INTO attestation_events(tenant_id,attestation_id,actor,ledger_revision) VALUES($1,$2,$3,$4)`, t.tenant, t.pendingWithdrawal, e.Caller.Principal, e.LedgerRevision); err != nil {
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
		if _, err = t.tx.Exec(ctx, `INSERT INTO fact_snapshots VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, s.ID, s.SelectionRevision, s.CreationRevision, s.Digest, b); err != nil {
			return err
		}
		for i, item := range s.Items {
			var record, version *string
			if item.Resolution.RecordID != "" {
				v := item.Resolution.RecordID
				record = &v
			}
			if item.Resolution.VersionID != "" {
				v := item.Resolution.VersionID
				version = &v
			}
			if _, err = t.tx.Exec(ctx, `INSERT INTO fact_snapshot_items VALUES($1,$2,$3,$4,$5,$6,$7)`, t.tenant, s.ID, i, item.Selector.Scope, model.Digest(item.Selector), record, version); err != nil {
				return err
			}
		}
	}
	return nil
}
func (t *transaction) Acceptances(ctx context.Context, record string, revision int64) ([]model.Acceptance, error) {
	query := `SELECT payload FROM accepted_intervals WHERE tenant_id=$1 AND record_id=$2 ORDER BY valid_from,segment_id`
	args := []any{t.tenant, record}
	if revision > 0 {
		query = `SELECT payload FROM timeline_history WHERE tenant_id=$1 AND record_id=$2 AND ledger_revision=(SELECT max(ledger_revision) FROM timeline_generations WHERE tenant_id=$1 AND record_id=$2 AND ledger_revision<=$3) ORDER BY ordinal`
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
	var yes bool
	err := t.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM rejected_versions WHERE tenant_id=$1 AND record_id=$2 AND version_id=$3 AND ($4::bigint=0 OR ledger_revision<=$4))`, t.tenant, record, version, revision).Scan(&yes)
	return yes, err
}
func (t *transaction) HasAcceptance(ctx context.Context, record, version string) (bool, error) {
	var yes bool
	err := t.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM timeline_history WHERE tenant_id=$1 AND record_id=$2 AND version_id=$3)`, t.tenant, record, version).Scan(&yes)
	return yes, err
}
func (t *transaction) Versions(ctx context.Context, record string, revision int64) ([]model.Version, error) {
	rows, err := t.tx.Query(ctx, `SELECT v.payload FROM record_versions v JOIN record_events r ON r.tenant_id=v.tenant_id AND r.record_id=v.record_id AND r.version_id=v.id JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id WHERE v.tenant_id=$1 AND v.record_id=$2 AND e.event_type='propose_fact' AND e.ledger_revision<=$3 ORDER BY e.ledger_revision`, t.tenant, record, revision)
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
		var v model.Version
		if err := t.decode(b, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (t *transaction) HistoricalAttestations(ctx context.Context, version string, revision int64) ([]model.Attestation, error) {
	// Fail explicitly when upgrading a ledger that cannot place old attestations in revision order.
	var gap bool
	err := t.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM attestations a WHERE tenant_id=$1 AND version_id=$2 AND (ledger_revision IS NULL OR EXISTS(SELECT 1 FROM attestation_events e WHERE e.tenant_id=a.tenant_id AND e.attestation_id=a.id AND e.ledger_revision IS NULL)))`, t.tenant, version).Scan(&gap)
	if err != nil {
		return nil, err
	}
	if gap {
		return nil, model.ErrPrecondition
	}
	rows, err := t.tx.Query(ctx, `SELECT a.payload FROM attestations a WHERE a.tenant_id=$1 AND a.version_id=$2 AND a.ledger_revision<=$3 AND NOT EXISTS(SELECT 1 FROM attestation_events e WHERE e.tenant_id=a.tenant_id AND e.attestation_id=a.id AND e.ledger_revision<=$3) ORDER BY a.id`, t.tenant, version, revision)
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
		var a model.Attestation
		if err := t.decode(b, &a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (t *transaction) RevisionTime(ctx context.Context, rev int64) (time.Time, error) {
	var at time.Time
	err := t.tx.QueryRow(ctx, `SELECT recorded_at FROM ledger_events WHERE tenant_id=$1 AND ledger_revision=$2`, t.tenant, rev).Scan(&at)
	return at, translate(err)
}
func (t *transaction) StreamRevisionAt(ctx context.Context, record string, revision int64) (int64, error) {
	var rev int64
	err := t.tx.QueryRow(ctx, `SELECT coalesce(max(r.stream_revision),0) FROM record_events r JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id WHERE r.tenant_id=$1 AND r.record_id=$2 AND e.ledger_revision<=$3`, t.tenant, record, revision).Scan(&rev)
	return rev, err
}
func (t *transaction) Snapshot(ctx context.Context, id string) (model.FactSnapshot, error) {
	var s model.FactSnapshot
	err := t.read(ctx, &s, `SELECT payload FROM fact_snapshots WHERE tenant_id=$1 AND id=$2`, t.tenant, id)
	return s, err
}
func (t *transaction) History(ctx context.Context, record string) ([]model.Event, error) {
	rows, err := t.tx.Query(ctx, `SELECT e.payload,e.recorded_at FROM ledger_events e JOIN record_events r ON r.tenant_id=e.tenant_id AND r.event_id=e.id WHERE e.tenant_id=$1 AND r.record_id=$2 ORDER BY e.ledger_revision LIMIT 1001`, t.tenant, record)
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
