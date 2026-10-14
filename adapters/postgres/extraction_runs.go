package postgres

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"time"
)

func (t *transaction) BeginExtraction(ctx context.Context, id, attempt string, concurrent int, daily, reservation int64, lease time.Duration, cost model.ExtractionCostBudget) (model.ExtractionAdmission, error) {
	if concurrent < 1 || concurrent > 32 || reservation < 1 || daily < reservation || lease < time.Second || lease > 5*time.Minute || (cost.Unit == "" && (cost.DailyLimit != 0 || cost.PerAttempt != 0)) || (cost.Unit != "" && (!model.ValidID(cost.Unit) || cost.PerAttempt < 1 || cost.DailyLimit < cost.PerAttempt)) {
		return model.ExtractionAdmission{}, model.ErrInvalid
	}
	var reserved, reservedCost int64
	var unit, day string
	// One tenant row serializes token/cost reservations and live-lease admission.
	err := t.tx.QueryRow(ctx, `INSERT INTO extraction_quota(tenant_id,day,reserved_tokens,epoch) VALUES($1,(clock_timestamp() AT TIME ZONE 'UTC')::date,0,1) ON CONFLICT(tenant_id) DO UPDATE SET epoch=extraction_quota.epoch+1,day=EXCLUDED.day,reserved_tokens=CASE WHEN extraction_quota.day=EXCLUDED.day THEN extraction_quota.reserved_tokens ELSE 0 END,reserved_cost_units=CASE WHEN extraction_quota.day=EXCLUDED.day THEN extraction_quota.reserved_cost_units ELSE 0 END RETURNING reserved_tokens,reserved_cost_units,cost_unit,to_char(day,'YYYY-MM-DD')`, t.tenant).Scan(&reserved, &reservedCost, &unit, &day)
	if err != nil {
		return model.ExtractionAdmission{}, err
	}
	// Never mix units or allow an unpriced worker to bypass an enabled meter.
	// Enabling cost accounting after unpriced attempts requires a new UTC day.
	if (unit != "" && unit != cost.Unit) || (unit == "" && cost.Unit != "" && reserved > 0) {
		return model.ExtractionAdmission{}, model.ErrPrecondition
	}
	if reserved > daily-reservation || (cost.Unit != "" && reservedCost > cost.DailyLimit-cost.PerAttempt) {
		return model.ExtractionAdmission{}, model.ErrQuota
	}
	var active int
	if err = t.tx.QueryRow(ctx, `SELECT count(*) FROM extraction_job_state WHERE tenant_id=$1 AND state='running' AND lease_until>clock_timestamp()`, t.tenant).Scan(&active); err != nil {
		return model.ExtractionAdmission{}, err
	}
	if active >= concurrent {
		return model.ExtractionAdmission{}, model.ErrQuota
	}
	var claimed string
	err = t.tx.QueryRow(ctx, `UPDATE extraction_job_state SET state='running',fence=fence+1,attempts=attempts+1,attempt_id=$3,lease_until=clock_timestamp()+$4::bigint*interval '1 millisecond',last_failure_code='' WHERE tenant_id=$1 AND id=$2 AND attempts<max_attempts AND ((state='pending' AND available_at<=clock_timestamp()) OR (state='running' AND lease_until<=clock_timestamp())) RETURNING id`, t.tenant, id, attempt, lease.Milliseconds()).Scan(&claimed)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ExtractionAdmission{}, model.ErrUnavailable
	}
	if err != nil {
		return model.ExtractionAdmission{}, err
	}
	if _, err = t.tx.Exec(ctx, `UPDATE extraction_quota SET reserved_tokens=reserved_tokens+$2,reserved_cost_units=reserved_cost_units+$3,cost_unit=$4 WHERE tenant_id=$1`, t.tenant, reservation, cost.PerAttempt, cost.Unit); err != nil {
		return model.ExtractionAdmission{}, err
	}
	status, err := t.ExtractionJobStatus(ctx, id)
	return model.ExtractionAdmission{JobStatus: status, Reservation: model.ExtractionReservation{Day: day, Tokens: reservation, DailyTokenLimit: daily, ConcurrentLimit: concurrent, CostUnit: cost.Unit, CostUnits: cost.PerAttempt, DailyCostLimit: cost.DailyLimit}}, err
}
func (t *transaction) StageExtractionAttempt(_ context.Context, a model.ExtractionAttempt) error {
	if a.Tenant != t.tenant {
		return model.ErrInvalid
	}
	t.pendingExtractionAttempt = &a
	return nil
}
func (t *transaction) ExtractionAttempt(ctx context.Context, id string) (model.ExtractionAttempt, error) {
	var a model.ExtractionAttempt
	err := t.read(ctx, &a, `SELECT payload FROM extraction_attempts WHERE tenant_id=$1 AND id=$2`, t.tenant, id)
	return a, err
}
func (t *transaction) ExtractionResult(ctx context.Context, id string) (model.ExtractionResult, error) {
	var r model.ExtractionResult
	err := t.read(ctx, &r, `SELECT payload FROM extraction_results WHERE tenant_id=$1 AND attempt_id=$2`, t.tenant, id)
	return r, err
}
func (t *transaction) FinishExtraction(ctx context.Context, job, attempt string, fence int64, state, code string, delay time.Duration) error {
	if state != "completed" && state != "pending" && state != "failed" {
		return model.ErrInvalid
	}
	done, err := t.tx.Exec(ctx, `UPDATE extraction_job_state SET state=CASE WHEN $5='pending' AND attempts>=max_attempts THEN 'failed' ELSE $5 END,lease_until=NULL,last_failure_code=$6,available_at=clock_timestamp()+$7::bigint*interval '1 millisecond' WHERE tenant_id=$1 AND id=$2 AND attempt_id=$3 AND fence=$4 AND state='running' AND lease_until>clock_timestamp()`, t.tenant, job, attempt, fence, state, code, delay.Milliseconds())
	if err != nil {
		return err
	}
	if done.RowsAffected() != 1 {
		return model.ErrRevision
	}
	return nil
}
func (t *transaction) StageExtractionResult(_ context.Context, r model.ExtractionResult) error {
	if r.Tenant != t.tenant {
		return model.ErrInvalid
	}
	t.pendingExtractionResult = &r
	return nil
}
func (t *transaction) flushExtractionRun(ctx context.Context, e model.Event) error {
	if t.pendingExtractionAttempt != nil {
		a := *t.pendingExtractionAttempt
		a.LedgerRevision = e.LedgerRevision
		a.StartedAt = e.RecordedAt
		b, err := t.encode(a)
		if err != nil {
			return err
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_attempts VALUES($1,$2,$3,$4,$5,$6,$7)`, t.tenant, a.ID, a.JobID, a.Scope, a.Fence, e.LedgerRevision, b); err != nil {
			return err
		}
	}
	if t.pendingExtractionResult != nil {
		r := *t.pendingExtractionResult
		r.LedgerRevision = e.LedgerRevision
		r.CompletedAt = e.RecordedAt
		b, err := t.encode(r)
		if err != nil {
			return err
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_results VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, r.AttemptID, r.JobID, r.Scope, e.LedgerRevision, b); err != nil {
			return err
		}
		for _, p := range r.Proposals {
			if p.RecordKind == "memory" {
				if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_memory_proposals VALUES($1,$2,$3,$4,$5)`, t.tenant, r.AttemptID, p.Candidate, p.RecordID, p.VersionID); err != nil {
					return err
				}
				continue
			}
			if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_proposals VALUES($1,$2,$3,$4,$5)`, t.tenant, r.AttemptID, p.Candidate, p.RecordID, p.VersionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *transaction) FailExpiredExtraction(ctx context.Context, id string, fence int64) error {
	done, err := t.tx.Exec(ctx, `UPDATE extraction_job_state SET state='failed',lease_until=NULL,last_failure_code='lease_exhausted' WHERE tenant_id=$1 AND id=$2 AND fence=$3 AND state='running' AND attempts>=max_attempts AND lease_until<=clock_timestamp()`, t.tenant, id, fence)
	if err != nil {
		return err
	}
	if done.RowsAffected() != 1 {
		return model.ErrRevision
	}
	return nil
}
func (t *transaction) RejectExtractionInput(ctx context.Context, id string) error {
	done, err := t.tx.Exec(ctx, `UPDATE extraction_job_state SET state='failed',fence=fence+1,lease_until=NULL,last_failure_code='input_limit' WHERE tenant_id=$1 AND id=$2 AND (state='pending' OR (state='running' AND lease_until<=clock_timestamp()))`, t.tenant, id)
	if err != nil {
		return err
	}
	if done.RowsAffected() != 1 {
		return model.ErrRevision
	}
	return nil
}
