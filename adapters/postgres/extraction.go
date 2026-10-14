package postgres

import (
	"context"
	"math"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgtype"
)

func (t *transaction) StageExtractionJob(ctx context.Context, j model.ExtractionJob) error {
	if j.Tenant != t.tenant {
		return model.ErrInvalid
	}
	if err := t.lockRetentionScope(ctx, j.Request.Scope); err != nil {
		return err
	}
	for _, evidenceID := range j.Request.RedactedEvidenceIDs {
		evidence, err := t.Evidence(ctx, evidenceID)
		if err != nil {
			return err
		}
		if evidence.Scope != j.Request.Scope || evidence.Derivation == nil {
			return model.ErrPrecondition
		}
		if err := t.guardEvidenceReference(ctx, evidenceID); err != nil {
			return err
		}
	}
	t.pendingExtraction = &j
	return nil
}
func (t *transaction) ExtractionJob(ctx context.Context, id string) (model.ExtractionJob, error) {
	var j model.ExtractionJob
	payload, err := ledgerdb.New(t.tx).GetExtractionJobPayload(ctx, ledgerdb.GetExtractionJobPayloadParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return j, translate(err)
	}
	err = t.decode(payload, &j)
	return j, err
}
func (t *transaction) ExtractionJobStatus(ctx context.Context, id string) (model.JobStatus, error) {
	s := model.JobStatus{ID: id, Kind: "extraction"}
	row, err := ledgerdb.New(t.tx).GetExtractionJobStatus(ctx, ledgerdb.GetExtractionJobStatusParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return s, translate(err)
	}
	if !row.AvailableAt.Valid || row.AvailableAt.InfinityModifier != pgtype.Finite || (row.LeaseUntil.Valid && row.LeaseUntil.InfinityModifier != pgtype.Finite) {
		return s, model.ErrPrecondition
	}
	s.Scope, s.CreatedRevision, s.State, s.Fence = row.ScopeID, row.LedgerRevision, row.State, row.Fence
	s.Attempts, s.MaxAttempts = int(row.Attempts), int(row.MaxAttempts)
	s.AttemptID, s.AvailableAt, s.LastFailureCode = row.AttemptID, row.AvailableAt.Time, row.LastFailureCode
	if row.LeaseUntil.Valid {
		s.LeaseUntil = &row.LeaseUntil.Time
	}
	return s, nil
}
func (t *transaction) RetryExtractionJob(ctx context.Context, id string, fence int64) error {
	result, err := ledgerdb.New(t.tx).RetryExtractionJob(ctx, ledgerdb.RetryExtractionJobParams{TenantID: t.tenant, ID: id, Fence: fence})
	if err != nil {
		return err
	}
	if result != 1 {
		return model.ErrRevision
	}
	t.pendingExtractionRetry = id
	return nil
}
func (t *transaction) flushExtraction(ctx context.Context, e model.Event) error {
	if t.pendingExtraction != nil {
		j := *t.pendingExtraction
		if j.Profile.MaxAttempts < 0 || j.Profile.MaxAttempts > math.MaxInt32 {
			return model.ErrInvalid
		}
		j.CreatedRevision = e.LedgerRevision
		b, err := t.encode(j)
		if err != nil {
			return err
		}
		var memoryRecord, memoryVersion pgtype.Text
		if j.Request.Refresh != nil {
			memoryRecord = pgtype.Text{String: j.Request.Refresh.RecordID, Valid: true}
			memoryVersion = pgtype.Text{String: j.Request.Refresh.VersionID, Valid: true}
		}
		if err = ledgerdb.New(t.tx).InsertExtractionJob(ctx, ledgerdb.InsertExtractionJobParams{TenantID: t.tenant, ID: j.ID, ScopeID: j.Request.Scope, ConversationID: nullableID(j.Request.ConversationID), ProfileID: j.Profile.ID, ProfileDigest: j.ProfileDigest, LedgerRevision: e.LedgerRevision, Payload: b, MemoryRecordID: memoryRecord, MemoryVersionID: memoryVersion}); err != nil {
			return err
		}
		for _, evidenceID := range j.Request.RedactedEvidenceIDs {
			if err = ledgerdb.New(t.tx).InsertExtractionRedaction(ctx, ledgerdb.InsertExtractionRedactionParams{TenantID: t.tenant, JobID: j.ID, ScopeID: j.Request.Scope, EvidenceID: evidenceID}); err != nil {
				return err
			}
		}
		for _, subject := range j.Request.Subjects {
			if err = ledgerdb.New(t.tx).InsertExtractionSubject(ctx, ledgerdb.InsertExtractionSubjectParams{TenantID: t.tenant, JobID: j.ID, ScopeID: j.Request.Scope, SubjectID: subject}); err != nil {
				return err
			}
		}
		for _, target := range j.Request.MemoryTargets {
			if err = ledgerdb.New(t.tx).InsertExtractionMemoryTarget(ctx, ledgerdb.InsertExtractionMemoryTargetParams{TenantID: t.tenant, JobID: j.ID, ID: target.ID, ScopeID: j.Request.Scope, Subtype: target.Subtype, SchemaVersion: target.SchemaVersion}); err != nil {
				return err
			}
			for _, d := range target.Dependencies {
				if err = ledgerdb.New(t.tx).InsertExtractionMemoryDependency(ctx, ledgerdb.InsertExtractionMemoryDependencyParams{TenantID: t.tenant, JobID: j.ID, TargetID: target.ID, ScopeID: d.Scope, RecordID: d.RecordID, VersionID: d.VersionID}); err != nil {
					return err
				}
			}
		}
		if j.Request.RefreshTrigger != nil {
			if err = ledgerdb.New(t.tx).InsertExtractionRefreshTrigger(ctx, ledgerdb.InsertExtractionRefreshTriggerParams{TenantID: t.tenant, JobID: j.ID, EventID: j.Request.RefreshTrigger.EventID}); err != nil {
				return err
			}
		}
		if err = ledgerdb.New(t.tx).InsertExtractionJobState(ctx, ledgerdb.InsertExtractionJobStateParams{TenantID: t.tenant, ID: j.ID, MaxAttempts: int32(j.Profile.MaxAttempts)}); err != nil {
			return err
		}
	}
	if t.pendingExtractionRetry != "" {
		b, err := t.encode(e)
		if err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertExtractionRetryEvent(ctx, ledgerdb.InsertExtractionRetryEventParams{TenantID: t.tenant, ID: e.ID, JobID: t.pendingExtractionRetry, LedgerRevision: e.LedgerRevision, Payload: b}); err != nil {
			return err
		}
	}
	return nil
}
