package postgres

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
)

func (t *transaction) BeginExtraction(ctx context.Context, id, attempt string, concurrent int, daily, reservation int64, lease time.Duration, cost model.ExtractionCostBudget) (model.ExtractionAdmission, error) {
	if concurrent < 1 || concurrent > 32 || reservation < 1 || daily < reservation || lease < time.Second || lease > 5*time.Minute || (cost.Unit == "" && (cost.DailyLimit != 0 || cost.PerAttempt != 0)) || (cost.Unit != "" && (!model.ValidID(cost.Unit) || cost.PerAttempt < 1 || cost.DailyLimit < cost.PerAttempt)) {
		return model.ExtractionAdmission{}, model.ErrInvalid
	}
	// One tenant row serializes token/cost reservations and live-lease admission.
	quota, err := ledgerdb.New(t.tx).LockExtractionQuota(ctx, t.tenant)
	if err != nil {
		return model.ExtractionAdmission{}, err
	}
	// Never mix units or allow an unpriced worker to bypass an enabled meter.
	// Enabling cost accounting after unpriced attempts requires a new UTC day.
	if (quota.CostUnit != "" && quota.CostUnit != cost.Unit) || (quota.CostUnit == "" && cost.Unit != "" && quota.ReservedTokens > 0) {
		return model.ExtractionAdmission{}, model.ErrPrecondition
	}
	if quota.ReservedTokens > daily-reservation || (cost.Unit != "" && quota.ReservedCostUnits > cost.DailyLimit-cost.PerAttempt) {
		return model.ExtractionAdmission{}, model.ErrQuota
	}
	active, err := ledgerdb.New(t.tx).CountActiveExtractions(ctx, t.tenant)
	if err != nil {
		return model.ExtractionAdmission{}, err
	}
	if active >= int64(concurrent) {
		return model.ExtractionAdmission{}, model.ErrQuota
	}
	_, err = ledgerdb.New(t.tx).ClaimExtractionJob(ctx, ledgerdb.ClaimExtractionJobParams{TenantID: t.tenant, ID: id, AttemptID: attempt, LeaseMillis: lease.Milliseconds()})
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ExtractionAdmission{}, model.ErrUnavailable
	}
	if err != nil {
		return model.ExtractionAdmission{}, err
	}
	if err = ledgerdb.New(t.tx).ReserveExtractionQuota(ctx, ledgerdb.ReserveExtractionQuotaParams{TenantID: t.tenant, ReservedTokens: reservation, ReservedCostUnits: cost.PerAttempt, CostUnit: cost.Unit}); err != nil {
		return model.ExtractionAdmission{}, err
	}
	status, err := t.ExtractionJobStatus(ctx, id)
	return model.ExtractionAdmission{JobStatus: status, Reservation: model.ExtractionReservation{Day: quota.QuotaDay, Tokens: reservation, DailyTokenLimit: daily, ConcurrentLimit: concurrent, CostUnit: cost.Unit, CostUnits: cost.PerAttempt, DailyCostLimit: cost.DailyLimit}}, err
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
	payload, err := ledgerdb.New(t.tx).GetExtractionAttemptPayload(ctx, ledgerdb.GetExtractionAttemptPayloadParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return a, translate(err)
	}
	if len(payload) == 0 {
		return a, model.ErrPrecondition
	}
	err = t.decode(payload, &a)
	return a, err
}
func (t *transaction) ExtractionResult(ctx context.Context, id string) (model.ExtractionResult, error) {
	var r model.ExtractionResult
	payload, err := ledgerdb.New(t.tx).GetExtractionResultPayload(ctx, ledgerdb.GetExtractionResultPayloadParams{TenantID: t.tenant, AttemptID: id})
	if err != nil {
		return r, translate(err)
	}
	if len(payload) == 0 {
		return r, model.ErrPrecondition
	}
	err = t.decode(payload, &r)
	return r, err
}
func (t *transaction) FinishExtraction(ctx context.Context, job, attempt string, fence int64, state, code string, delay time.Duration) error {
	if state != "completed" && state != "pending" && state != "failed" {
		return model.ErrInvalid
	}
	done, err := ledgerdb.New(t.tx).FinishExtractionJob(ctx, ledgerdb.FinishExtractionJobParams{TenantID: t.tenant, ID: job, AttemptID: attempt, Fence: fence, NextState: state, LastFailureCode: code, DelayMillis: delay.Milliseconds()})
	if err != nil {
		return err
	}
	if done != 1 {
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
		if err = ledgerdb.New(t.tx).InsertExtractionAttemptHeader(ctx, ledgerdb.InsertExtractionAttemptHeaderParams{TenantID: t.tenant, ID: a.ID, JobID: a.JobID, ScopeID: a.Scope, Fence: a.Fence, LedgerRevision: e.LedgerRevision}); err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertExtractionAttemptPayload(ctx, ledgerdb.InsertExtractionAttemptPayloadParams{TenantID: t.tenant, AttemptID: a.ID, Payload: b}); err != nil {
			return err
		}
		if err := t.retainExtractionInputInventory(ctx, a); err != nil {
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
		if err = ledgerdb.New(t.tx).InsertExtractionResultHeader(ctx, ledgerdb.InsertExtractionResultHeaderParams{TenantID: t.tenant, AttemptID: r.AttemptID, JobID: r.JobID, ScopeID: r.Scope, LedgerRevision: e.LedgerRevision}); err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertExtractionResultPayload(ctx, ledgerdb.InsertExtractionResultPayloadParams{TenantID: t.tenant, AttemptID: r.AttemptID, Payload: b}); err != nil {
			return err
		}
		for _, p := range r.Proposals {
			if p.Candidate < 0 || p.Candidate > math.MaxInt32 {
				return model.ErrInvalid
			}
			if p.RecordKind == "memory" {
				if err = ledgerdb.New(t.tx).InsertExtractionMemoryProposal(ctx, ledgerdb.InsertExtractionMemoryProposalParams{TenantID: t.tenant, AttemptID: r.AttemptID, Ordinal: int32(p.Candidate), RecordID: p.RecordID, VersionID: p.VersionID}); err != nil {
					return err
				}
				continue
			}
			if err = ledgerdb.New(t.tx).InsertExtractionFactProposal(ctx, ledgerdb.InsertExtractionFactProposalParams{TenantID: t.tenant, AttemptID: r.AttemptID, Ordinal: int32(p.Candidate), RecordID: p.RecordID, VersionID: p.VersionID}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *transaction) FailExpiredExtraction(ctx context.Context, id string, fence int64) error {
	done, err := ledgerdb.New(t.tx).FailExpiredExtraction(ctx, ledgerdb.FailExpiredExtractionParams{TenantID: t.tenant, ID: id, Fence: fence})
	if err != nil {
		return err
	}
	if done != 1 {
		return model.ErrRevision
	}
	return nil
}
func (t *transaction) RejectExtractionInput(ctx context.Context, id string) error {
	done, err := ledgerdb.New(t.tx).RejectExtractionInput(ctx, ledgerdb.RejectExtractionInputParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return err
	}
	if done != 1 {
		return model.ErrRevision
	}
	return nil
}
