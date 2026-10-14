package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) StageExtractionJob(ctx context.Context, j model.ExtractionJob) error {
	if j.Tenant != t.tenant {
		return model.ErrInvalid
	}
	if err := t.lockRetentionScope(ctx, j.Request.Scope); err != nil {
		return err
	}
	t.pendingExtraction = &j
	return nil
}
func (t *transaction) ExtractionJob(ctx context.Context, id string) (model.ExtractionJob, error) {
	var j model.ExtractionJob
	err := t.read(ctx, &j, `SELECT payload FROM extraction_jobs WHERE tenant_id=$1 AND id=$2`, t.tenant, id)
	return j, err
}
func (t *transaction) ExtractionJobStatus(ctx context.Context, id string) (model.JobStatus, error) {
	s := model.JobStatus{ID: id, Kind: "extraction"}
	err := t.tx.QueryRow(ctx, `SELECT j.scope_id,j.ledger_revision,s.state,s.fence,s.attempts,s.max_attempts,s.attempt_id,s.available_at,s.lease_until,s.last_failure_code FROM extraction_jobs j JOIN extraction_job_state s USING(tenant_id,id) WHERE j.tenant_id=$1 AND j.id=$2`, t.tenant, id).Scan(&s.Scope, &s.CreatedRevision, &s.State, &s.Fence, &s.Attempts, &s.MaxAttempts, &s.AttemptID, &s.AvailableAt, &s.LeaseUntil, &s.LastFailureCode)
	return s, translate(err)
}
func (t *transaction) RetryExtractionJob(ctx context.Context, id string, fence int64) error {
	result, err := t.tx.Exec(ctx, `UPDATE extraction_job_state SET state='pending',fence=fence+1,attempts=0,lease_until=NULL,available_at=clock_timestamp(),last_failure_code='' WHERE tenant_id=$1 AND id=$2 AND state='failed' AND fence=$3`, t.tenant, id, fence)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return model.ErrRevision
	}
	t.pendingExtractionRetry = id
	return nil
}
func (t *transaction) flushExtraction(ctx context.Context, e model.Event) error {
	if t.pendingExtraction != nil {
		j := *t.pendingExtraction
		j.CreatedRevision = e.LedgerRevision
		b, err := t.encode(j)
		if err != nil {
			return err
		}
		var memoryRecord, memoryVersion any
		if j.Request.Refresh != nil {
			memoryRecord = j.Request.Refresh.RecordID
			memoryVersion = j.Request.Refresh.VersionID
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_jobs(tenant_id,id,scope_id,conversation_id,profile_id,profile_digest,ledger_revision,payload,memory_record_id,memory_version_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, t.tenant, j.ID, j.Request.Scope, nullableID(j.Request.ConversationID), j.Profile.ID, j.ProfileDigest, e.LedgerRevision, b, memoryRecord, memoryVersion); err != nil {
			return err
		}
		for _, subject := range j.Request.Subjects {
			if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_subjects VALUES($1,$2,$3,$4)`, t.tenant, j.ID, j.Request.Scope, subject); err != nil {
				return err
			}
		}
		for _, target := range j.Request.MemoryTargets {
			if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_memory_targets VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, j.ID, target.ID, j.Request.Scope, target.Subtype, target.SchemaVersion); err != nil {
				return err
			}
			for _, d := range target.Dependencies {
				if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_memory_dependencies VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, j.ID, target.ID, d.Scope, d.RecordID, d.VersionID); err != nil {
					return err
				}
			}
		}
		if j.Request.RefreshTrigger != nil {
			if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_refresh_triggers VALUES($1,$2,$3)`, t.tenant, j.ID, j.Request.RefreshTrigger.EventID); err != nil {
				return err
			}
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_job_state(tenant_id,id,max_attempts) VALUES($1,$2,$3)`, t.tenant, j.ID, j.Profile.MaxAttempts); err != nil {
			return err
		}
	}
	if t.pendingExtractionRetry != "" {
		b, err := t.encode(e)
		if err != nil {
			return err
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO extraction_retry_events VALUES($1,$2,$3,$4,$5)`, t.tenant, e.ID, t.pendingExtractionRetry, e.LedgerRevision, b); err != nil {
			return err
		}
	}
	return nil
}
