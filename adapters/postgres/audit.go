package postgres

import (
	"context"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

func (t *transaction) StageCalculation(_ context.Context, r model.CalculationRecord, snapshots []model.FactSnapshot) error {
	t.pendingCalculation = &r
	t.calculationSnapshots = snapshots
	return nil
}
func (t *transaction) Calculation(ctx context.Context, id string) (model.CalculationRecord, error) {
	var r model.CalculationRecord
	err := t.read(ctx, &r, `SELECT payload FROM calculation_receipts WHERE tenant_id=$1 AND id=$2`, t.tenant, id)
	return r, err
}
func (t *transaction) CalculationRun(ctx context.Context, application, run string) (model.CalculationRecord, error) {
	lock := model.Digest([]string{t.tenant, "calculation", application, run})
	if _, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lock); err != nil {
		return model.CalculationRecord{}, err
	}
	var r model.CalculationRecord
	err := t.read(ctx, &r, `SELECT payload FROM calculation_receipts WHERE tenant_id=$1 AND application_id=$2 AND external_run_id=$3`, t.tenant, application, run)
	return r, err
}
func (t *transaction) StageAuditJob(_ context.Context, j model.AuditExportJob) error {
	t.pendingAuditJob = &j
	return nil
}
func (t *transaction) StageAuditBundle(_ context.Context, j model.AuditExportJob, b model.AuditBundle) error {
	if b.Manifest.ExportID != j.ID || b.Manifest.Tenant != t.tenant || b.Manifest.Scope != j.Request.Scope {
		return model.ErrInvalid
	}
	t.auditCompletionJob = &j
	t.pendingAuditBundle = &b
	return nil
}
func (t *transaction) AuditJob(ctx context.Context, id string) (model.AuditExportJob, error) {
	var j model.AuditExportJob
	var payload []byte
	var state string
	var fence int64
	var lease *time.Time
	err := t.tx.QueryRow(ctx, `SELECT r.payload,j.state,j.fence,j.lease_until FROM audit_export_requests r JOIN audit_export_jobs j ON j.tenant_id=r.tenant_id AND j.id=r.id WHERE r.tenant_id=$1 AND r.id=$2`, t.tenant, id).Scan(&payload, &state, &fence, &lease)
	if err != nil {
		return j, translate(err)
	}
	if err := t.decode(payload, &j); err != nil {
		return j, err
	}
	j.State = state
	j.Fence = fence
	j.LeaseUntil = lease
	return j, nil
}
func (t *transaction) ClaimAuditJob(ctx context.Context, id string, lease time.Duration) (model.AuditExportJob, error) {
	if lease <= 0 || lease > 5*time.Minute {
		return model.AuditExportJob{}, model.ErrInvalid
	}
	var claimed string
	err := t.tx.QueryRow(ctx, `UPDATE audit_export_jobs SET state='running',fence=fence+1,lease_until=clock_timestamp()+($3::bigint*interval '1 millisecond') WHERE tenant_id=$1 AND id=(SELECT id FROM audit_export_jobs WHERE tenant_id=$1 AND id=$2 AND (state='pending' OR (state='running' AND lease_until<clock_timestamp())) FOR UPDATE SKIP LOCKED) RETURNING id`, t.tenant, id, lease.Milliseconds()).Scan(&claimed)
	if err != nil {
		return model.AuditExportJob{}, translate(err)
	}
	return t.AuditJob(ctx, claimed)
}
func (t *transaction) AuditExport(ctx context.Context, id string) (model.AuditExportResult, error) {
	j, err := t.AuditJob(ctx, id)
	if err != nil {
		return model.AuditExportResult{}, err
	}
	r := model.AuditExportResult{ID: j.ID, State: j.State, CreatedRevision: j.CreatedRevision}
	if j.State != "completed" {
		return r, nil
	}
	var payload []byte
	var digest string
	err = t.tx.QueryRow(ctx, `SELECT payload,digest,ledger_revision FROM audit_exports WHERE tenant_id=$1 AND id=$2`, t.tenant, id).Scan(&payload, &digest, &r.CompletedRevision)
	if err != nil {
		return r, translate(err)
	}
	var bundle model.AuditBundle
	if err := t.decode(payload, &bundle); err != nil {
		return r, err
	}
	b, err := json.Marshal(bundle)
	if err != nil {
		return r, err
	}
	if model.BytesDigest(b) != digest {
		return r, model.ErrPrecondition
	}
	r.Bundle = &bundle
	r.Artifact = model.ObjectRef{Locator: "postgres:audit_exports/" + id, Version: digest, Digest: digest, Size: int64(len(b))}
	return r, nil
}
func (t *transaction) flushAudit(ctx context.Context, e model.Event) error {
	if t.pendingCalculation != nil {
		r := *t.pendingCalculation
		r.LedgerRevision = e.LedgerRevision
		b, err := t.encode(r)
		if err != nil {
			return err
		}
		if _, err := t.tx.Exec(ctx, `INSERT INTO calculation_receipts VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, t.tenant, r.ID, r.Receipt.Scope, r.Receipt.SnapshotID, r.Receipt.ApplicationID, r.Receipt.ExternalRunID, r.Digest, e.LedgerRevision, b); err != nil {
			return err
		}
		for _, snap := range t.calculationSnapshots {
			if _, err := t.tx.Exec(ctx, `INSERT INTO calculation_snapshots VALUES($1,$2,$3)`, t.tenant, r.ID, snap.ID); err != nil {
				return err
			}
		}
		for _, a := range []struct {
			ref  model.ArtifactReference
			role string
		}{{r.Receipt.Input, "input"}, {r.Receipt.Output, "output"}} {
			if _, err := t.tx.Exec(ctx, `INSERT INTO calculation_artifacts VALUES($1,$2,$3,$4)`, t.tenant, r.ID, a.ref.EvidenceID, a.role); err != nil {
				return err
			}
		}
	}
	if t.pendingAuditJob != nil {
		j := *t.pendingAuditJob
		j.CreatedRevision = e.LedgerRevision
		b, err := t.encode(j)
		if err != nil {
			return err
		}
		if _, err := t.tx.Exec(ctx, `INSERT INTO audit_export_requests VALUES($1,$2,$3,$4,$5)`, t.tenant, j.ID, j.Request.Scope, e.LedgerRevision, b); err != nil {
			return err
		}
		if _, err := t.tx.Exec(ctx, `INSERT INTO audit_export_jobs(tenant_id,id) VALUES($1,$2)`, t.tenant, j.ID); err != nil {
			return err
		}
	}
	if t.pendingAuditBundle != nil {
		job := *t.auditCompletionJob
		// Completion is fenced and checked again inside the ledger append transaction.
		tag, err := t.tx.Exec(ctx, `UPDATE audit_export_jobs SET state='completed',lease_until=NULL WHERE tenant_id=$1 AND id=$2 AND state='running' AND fence=$3 AND lease_until>clock_timestamp()`, t.tenant, job.ID, job.Fence)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return model.ErrRevision
		}
		plain, err := json.Marshal(t.pendingAuditBundle)
		if err != nil {
			return err
		}
		b, err := t.encode(t.pendingAuditBundle)
		if err != nil {
			return err
		}
		if _, err := t.tx.Exec(ctx, `INSERT INTO audit_exports VALUES($1,$2,$3,$4,$5)`, t.tenant, job.ID, e.LedgerRevision, model.BytesDigest(plain), b); err != nil {
			return err
		}
	}
	return nil
}

var _ model.AuditTx = (*transaction)(nil)
