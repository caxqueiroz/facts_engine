package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgtype"
)

func (t *transaction) StageCalculation(ctx context.Context, r model.CalculationRecord, snapshots []model.FactSnapshot) error {
	for _, ref := range []model.ArtifactReference{r.Receipt.Input, r.Receipt.Output} {
		if err := t.guardEvidenceReference(ctx, ref.EvidenceID); err != nil {
			return err
		}
	}
	t.pendingCalculation = &r
	t.calculationSnapshots = snapshots
	return nil
}
func (t *transaction) Calculation(ctx context.Context, id string) (model.CalculationRecord, error) {
	var r model.CalculationRecord
	payload, err := ledgerdb.New(t.tx).GetCalculationPayload(ctx, ledgerdb.GetCalculationPayloadParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return r, translate(err)
	}
	err = t.decode(payload, &r)
	return r, err
}
func (t *transaction) CalculationRun(ctx context.Context, application, run string) (model.CalculationRecord, error) {
	lock := model.Digest([]string{t.tenant, "calculation", application, run})
	if err := ledgerdb.New(t.tx).LockCalculationRun(ctx, lock); err != nil {
		return model.CalculationRecord{}, err
	}
	var r model.CalculationRecord
	payload, err := ledgerdb.New(t.tx).GetCalculationRunPayload(ctx, ledgerdb.GetCalculationRunPayloadParams{TenantID: t.tenant, ApplicationID: application, ExternalRunID: run})
	if err != nil {
		return r, translate(err)
	}
	err = t.decode(payload, &r)
	return r, err
}
func (t *transaction) StageAuditJob(ctx context.Context, j model.AuditExportJob) error {
	if err := t.lockRetentionScope(ctx, j.Request.Scope); err != nil {
		return err
	}
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
	row, err := ledgerdb.New(t.tx).GetAuditJob(ctx, ledgerdb.GetAuditJobParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return j, translate(err)
	}
	if err := t.decode(row.Payload, &j); err != nil {
		return j, err
	}
	j.State = row.State
	j.Fence = row.Fence
	if row.LeaseUntil.Valid {
		if row.LeaseUntil.InfinityModifier != pgtype.Finite {
			return j, model.ErrPrecondition
		}
		j.LeaseUntil = &row.LeaseUntil.Time
	} else {
		j.LeaseUntil = nil
	}
	return j, nil
}
func (t *transaction) ClaimAuditJob(ctx context.Context, id string, lease time.Duration) (model.AuditExportJob, error) {
	if lease <= 0 || lease > 5*time.Minute {
		return model.AuditExportJob{}, model.ErrInvalid
	}
	claimed, err := ledgerdb.New(t.tx).ClaimAuditJob(ctx, ledgerdb.ClaimAuditJobParams{TenantID: t.tenant, ID: id, LeaseMillis: lease.Milliseconds()})
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
	row, err := ledgerdb.New(t.tx).GetAuditExport(ctx, ledgerdb.GetAuditExportParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return r, translate(err)
	}
	var bundle model.AuditBundle
	if err := t.decode(row.Payload, &bundle); err != nil {
		return r, err
	}
	b, err := json.Marshal(bundle)
	if err != nil {
		return r, err
	}
	if model.BytesDigest(b) != row.Digest {
		return r, model.ErrPrecondition
	}
	r.CompletedRevision = row.LedgerRevision
	r.Bundle = &bundle
	r.Artifact = model.ObjectRef{Locator: "postgres:audit_exports/" + id, Version: row.Digest, Digest: row.Digest, Size: int64(len(b))}
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
		if err := ledgerdb.New(t.tx).InsertCalculationPayload(ctx, ledgerdb.InsertCalculationPayloadParams{TenantID: t.tenant, ID: r.ID, ScopeID: r.Receipt.Scope, SnapshotID: r.Receipt.SnapshotID, ApplicationID: r.Receipt.ApplicationID, ExternalRunID: r.Receipt.ExternalRunID, RequestDigest: r.Digest, LedgerRevision: e.LedgerRevision, Payload: b}); err != nil {
			return err
		}
		for _, snap := range t.calculationSnapshots {
			if err := ledgerdb.New(t.tx).InsertCalculationSnapshot(ctx, ledgerdb.InsertCalculationSnapshotParams{TenantID: t.tenant, CalculationID: r.ID, SnapshotID: snap.ID}); err != nil {
				return err
			}
		}
		for _, a := range []struct {
			ref  model.ArtifactReference
			role string
		}{{r.Receipt.Input, "input"}, {r.Receipt.Output, "output"}} {
			if err := ledgerdb.New(t.tx).InsertCalculationArtifact(ctx, ledgerdb.InsertCalculationArtifactParams{TenantID: t.tenant, CalculationID: r.ID, EvidenceID: a.ref.EvidenceID, Role: a.role}); err != nil {
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
		if err := ledgerdb.New(t.tx).InsertAuditRequest(ctx, ledgerdb.InsertAuditRequestParams{TenantID: t.tenant, ID: j.ID, ScopeID: j.Request.Scope, LedgerRevision: e.LedgerRevision, Payload: b}); err != nil {
			return err
		}
		if err := ledgerdb.New(t.tx).InsertAuditJob(ctx, ledgerdb.InsertAuditJobParams{TenantID: t.tenant, ID: j.ID}); err != nil {
			return err
		}
	}
	if t.pendingAuditBundle != nil {
		job := *t.auditCompletionJob
		// Completion is fenced and checked again inside the ledger append transaction.
		affected, err := ledgerdb.New(t.tx).CompleteAuditJob(ctx, ledgerdb.CompleteAuditJobParams{TenantID: t.tenant, ID: job.ID, Fence: job.Fence})
		if err != nil {
			return err
		}
		if affected != 1 {
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
		if err := ledgerdb.New(t.tx).InsertAuditExport(ctx, ledgerdb.InsertAuditExportParams{TenantID: t.tenant, ID: job.ID, LedgerRevision: e.LedgerRevision, Digest: model.BytesDigest(plain), Payload: b}); err != nil {
			return err
		}
	}
	return nil
}

var _ model.AuditTx = (*transaction)(nil)
