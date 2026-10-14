package postgres

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"time"
)

func validConsumer(k model.ConsumerKey) bool {
	return model.ValidID(k.Tenant) && model.ValidID(k.Consumer)
}
func validLease(d time.Duration) bool { return d >= time.Second && d <= 5*time.Minute }
func (l *Ledger) RegisterConsumer(ctx context.Context, k model.ConsumerKey, maxAttempts int) error {
	if !validConsumer(k) || maxAttempts < 1 || maxAttempts > 100 {
		return model.ErrInvalid
	}
	return l.Transact(ctx, k.Tenant, func(tx model.Tx) error {
		t := tx.(*transaction)
		if _, err := t.tx.Exec(ctx, `INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, k.Tenant, k.Consumer, maxAttempts); err != nil {
			return err
		}
		var configured int
		if err := t.tx.QueryRow(ctx, `SELECT max_attempts FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id=$2`, k.Tenant, k.Consumer).Scan(&configured); err != nil {
			return err
		}
		if configured != maxAttempts {
			return model.ErrConflict
		}
		return nil
	})
}
func (l *Ledger) ClaimDelivery(ctx context.Context, k model.ConsumerKey, lease time.Duration) (model.Delivery, error) {
	var d model.Delivery
	if !validConsumer(k) || !validLease(lease) {
		return d, model.ErrInvalid
	}
	exhausted := false
	err := l.Transact(ctx, k.Tenant, func(tx model.Tx) error {
		exhausted = false
		t := tx.(*transaction)
		var revision, fence int64
		var attempts, maxAttempts int
		err := t.tx.QueryRow(ctx, `SELECT acknowledged_revision,fence,attempts,max_attempts FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id=$2 AND ((state='pending' AND available_at<=clock_timestamp()) OR (state='running' AND lease_until<=clock_timestamp())) FOR UPDATE SKIP LOCKED`, k.Tenant, k.Consumer).Scan(&revision, &fence, &attempts, &maxAttempts)
		if err != nil {
			return err
		}
		if attempts >= maxAttempts {
			_, err = t.tx.Exec(ctx, `UPDATE consumer_offsets SET state='failed',lease_until=NULL,last_failure_code='lease_expired' WHERE tenant_id=$1 AND consumer_id=$2`, k.Tenant, k.Consumer)
			exhausted = true
			return err
		}
		var payload []byte
		var at time.Time
		err = t.tx.QueryRow(ctx, `SELECT e.payload,e.recorded_at FROM outbox o JOIN ledger_events e ON e.tenant_id=o.tenant_id AND e.id=o.event_id WHERE o.tenant_id=$1 AND o.ledger_revision=$2`, k.Tenant, revision+1).Scan(&payload, &at)
		if errors.Is(err, pgx.ErrNoRows) {
			current, revisionErr := tx.Revision(ctx)
			if revisionErr != nil {
				return revisionErr
			}
			if current > revision {
				return model.ErrPrecondition
			}
		}
		if err != nil {
			return err
		}
		d = model.Delivery{Key: k, Fence: fence + 1, Attempt: attempts + 1}
		if err = t.decode(payload, &d.Event); err != nil {
			return err
		}
		if d.Event.LedgerRevision != revision+1 || d.Event.Caller.Tenant != k.Tenant {
			return model.ErrPrecondition
		}
		d.Event.RecordedAt = at
		return t.tx.QueryRow(ctx, `UPDATE consumer_offsets SET state='running',fence=fence+1,attempts=attempts+1,lease_until=clock_timestamp()+($3::bigint*interval '1 millisecond') WHERE tenant_id=$1 AND consumer_id=$2 RETURNING lease_until`, k.Tenant, k.Consumer, lease.Milliseconds()).Scan(&d.LeaseUntil)
	})
	if exhausted && err == nil {
		err = model.ErrPrecondition
	}
	return d, err
}
func (l *Ledger) deliveryUpdate(ctx context.Context, d model.Delivery, query string, arg int64) error {
	if !validConsumer(d.Key) || d.Fence <= 0 || d.Event.LedgerRevision <= 0 || !model.ValidID(d.Event.ID) {
		return model.ErrInvalid
	}
	return l.Transact(ctx, d.Key.Tenant, func(tx model.Tx) error {
		tag, err := tx.(*transaction).tx.Exec(ctx, query, d.Key.Tenant, d.Key.Consumer, d.Fence, d.Event.LedgerRevision, d.Event.ID, arg)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return model.ErrRevision
		}
		return nil
	})
}

const deliveryFence = ` WHERE tenant_id=$1 AND consumer_id=$2 AND fence=$3 AND acknowledged_revision=$4-1 AND state='running' AND lease_until>clock_timestamp() AND EXISTS(SELECT 1 FROM outbox WHERE tenant_id=$1 AND event_id=$5 AND ledger_revision=$4)`

func (l *Ledger) RenewDelivery(ctx context.Context, d model.Delivery, lease time.Duration) error {
	if !validLease(lease) {
		return model.ErrInvalid
	}
	return l.deliveryUpdate(ctx, d, `UPDATE consumer_offsets SET lease_until=clock_timestamp()+($6::bigint*interval '1 millisecond')`+deliveryFence, lease.Milliseconds())
}
func (l *Ledger) AcknowledgeDelivery(ctx context.Context, d model.Delivery) error {
	return l.deliveryUpdate(ctx, d, `UPDATE consumer_offsets SET acknowledged_revision=$4,state='pending',attempts=0,lease_until=NULL,available_at=clock_timestamp(),last_failure_code=''`+deliveryFence+` AND $6::bigint=0`, 0)
}
func (l *Ledger) FailDelivery(ctx context.Context, d model.Delivery, backoff time.Duration) error {
	if backoff < time.Millisecond || backoff > 24*time.Hour {
		return model.ErrInvalid
	}
	return l.deliveryUpdate(ctx, d, `UPDATE consumer_offsets SET state=CASE WHEN attempts>=max_attempts THEN 'failed' ELSE 'pending' END,lease_until=NULL,available_at=clock_timestamp()+($6::bigint*interval '1 millisecond'),last_failure_code='handler_failed'`+deliveryFence, backoff.Milliseconds())
}
func (l *Ledger) ConsumerStatus(ctx context.Context, k model.ConsumerKey) (model.ConsumerStatus, error) {
	s := model.ConsumerStatus{Key: k}
	if !validConsumer(k) {
		return s, model.ErrInvalid
	}
	err := l.Transact(ctx, k.Tenant, func(tx model.Tx) error {
		return tx.(*transaction).tx.QueryRow(ctx, `SELECT acknowledged_revision,state,fence,attempts,max_attempts,lease_until,available_at,last_failure_code FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id=$2`, k.Tenant, k.Consumer).Scan(&s.AcknowledgedRevision, &s.State, &s.Fence, &s.Attempts, &s.MaxAttempts, &s.LeaseUntil, &s.AvailableAt, &s.LastFailureCode)
	})
	return s, err
}

// RetryConsumer is a trusted operator command, fenced against a specific failure.
// Its immutable receipt makes retries safe after a lost response.
func (l *Ledger) RetryConsumer(ctx context.Context, k model.ConsumerKey, failedFence int64) error {
	if !validConsumer(k) || failedFence <= 0 {
		return model.ErrInvalid
	}
	return l.Transact(ctx, k.Tenant, func(tx model.Tx) error {
		t := tx.(*transaction)
		var exists bool
		if err := t.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM consumer_retry_commands WHERE tenant_id=$1 AND consumer_id=$2 AND failed_fence=$3)`, k.Tenant, k.Consumer, failedFence).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
		tag, err := t.tx.Exec(ctx, `UPDATE consumer_offsets SET state='pending',attempts=0,lease_until=NULL,available_at=clock_timestamp(),fence=fence+1 WHERE tenant_id=$1 AND consumer_id=$2 AND state='failed' AND fence=$3`, k.Tenant, k.Consumer, failedFence)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return model.ErrRevision
		}
		_, err = t.tx.Exec(ctx, `INSERT INTO consumer_retry_commands(tenant_id,consumer_id,failed_fence) VALUES($1,$2,$3)`, k.Tenant, k.Consumer, failedFence)
		return err
	})
}
func (t *transaction) Changes(ctx context.Context, scope string, after, through int64, limit int) ([]model.Event, error) {
	if !model.ValidID(scope) || after < 0 || through < after || limit < 1 || limit > 1000 {
		return nil, model.ErrInvalid
	}
	rows, err := t.tx.Query(ctx, `SELECT payload,recorded_at FROM ledger_events WHERE tenant_id=$1 AND scope_id=$2 AND ledger_revision>$3 AND ledger_revision<=$4 ORDER BY ledger_revision LIMIT $5`, t.tenant, scope, after, through, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []model.Event{}
	for rows.Next() {
		var payload []byte
		var at time.Time
		if err := rows.Scan(&payload, &at); err != nil {
			return nil, err
		}
		var e model.Event
		if err := t.decode(payload, &e); err != nil {
			return nil, err
		}
		e.RecordedAt = at
		events = append(events, e)
	}
	return events, rows.Err()
}

var _ model.Outbox = (*Ledger)(nil)
