// Package factsengine exposes the embeddable, policy-governed fact engine.
package factsengine

import (
	"context"
	"github.com/caxqueiroz/facts_engine/internal/app/services"
	"github.com/caxqueiroz/facts_engine/model"
)

// Engine uses injected dependencies. New does not migrate, start goroutines, or own connections.
type Engine struct{ service *services.Service }

func New(ledger Ledger, auth Authorizer, evidence EvidenceStore) (*Engine, error) {
	s, err := services.New(ledger, auth, evidence)
	if err != nil {
		return nil, err
	}
	return &Engine{s}, nil
}
func (e *Engine) RegisterAcceptancePolicy(ctx context.Context, c Caller, r PolicyRequest) (CommitResult, error) {
	return e.service.RegisterAcceptancePolicy(ctx, c, r)
}
func (e *Engine) RegisterPredicateVersion(ctx context.Context, c Caller, r PredicateRequest) (CommitResult, error) {
	return e.service.RegisterPredicateVersion(ctx, c, r)
}
func (e *Engine) RegisterEvidence(ctx context.Context, c Caller, r EvidenceRequest) (CommitResult, error) {
	return e.service.RegisterEvidence(ctx, c, r)
}
func (e *Engine) ProposeFact(ctx context.Context, c Caller, r FactProposal) (CommitResult, error) {
	return e.service.ProposeFact(ctx, c, r)
}
func (e *Engine) RecordAttestation(ctx context.Context, c Caller, r AttestationRequest) (CommitResult, error) {
	return e.service.RecordAttestation(ctx, c, r)
}
func (e *Engine) WithdrawAttestation(ctx context.Context, c Caller, r Withdrawal) (CommitResult, error) {
	return e.service.WithdrawAttestation(ctx, c, r)
}
func (e *Engine) ApproveFact(ctx context.Context, c Caller, r Approval) (CommitResult, error) {
	return e.service.ApproveFact(ctx, c, r)
}
func (e *Engine) AcceptFact(ctx context.Context, c Caller, r Approval) (CommitResult, error) {
	return e.service.AcceptFact(ctx, c, r)
}
func (e *Engine) GetFact(ctx context.Context, c Caller, r FactQuery) (FactResult, error) {
	return e.service.GetFact(ctx, c, r)
}

type Caller = model.Caller
type Command = model.Command
type FactProposal = model.FactProposal
type Approval = model.Approval
type FactQuery = model.FactQuery
type FactResult = model.FactResult
type CommitResult = model.CommitResult
type PredicateRequest = model.PredicateRequest
type PolicyRequest = model.PolicyRequest
type EvidenceRequest = model.EvidenceRequest
type AttestationRequest = model.AttestationRequest
type Withdrawal = model.Withdrawal
type Ledger = model.Ledger
type Authorizer = model.Authorizer
type EvidenceStore = model.EvidenceStore
type Value = model.Value
type FactKey = model.FactKey
type Interval = model.Interval

var (
	ErrInvalid      = model.ErrInvalid
	ErrDenied       = model.ErrDenied
	ErrNotFound     = model.ErrNotFound
	ErrConflict     = model.ErrConflict
	ErrRevision     = model.ErrRevision
	ErrPrecondition = model.ErrPrecondition
	ErrUnavailable  = model.ErrUnavailable
)

func (e *Engine) SupersedeFact(ctx context.Context, c Caller, r Supersession) (CommitResult, error) {
	return e.service.SupersedeFact(ctx, c, r)
}
func (e *Engine) RejectFact(ctx context.Context, c Caller, r LifecycleChange) (CommitResult, error) {
	return e.service.RejectFact(ctx, c, r)
}
func (e *Engine) DisputeFact(ctx context.Context, c Caller, r LifecycleChange) (CommitResult, error) {
	return e.service.DisputeFact(ctx, c, r)
}
func (e *Engine) ResolveDispute(ctx context.Context, c Caller, r LifecycleChange) (CommitResult, error) {
	return e.service.ResolveDispute(ctx, c, r)
}
func (e *Engine) RetractFact(ctx context.Context, c Caller, r LifecycleChange) (CommitResult, error) {
	return e.service.RetractFact(ctx, c, r)
}
func (e *Engine) GetFactHistory(ctx context.Context, c Caller, r HistoryQuery) (HistoryResult, error) {
	return e.service.GetFactHistory(ctx, c, r)
}
func (e *Engine) CreateFactSnapshot(ctx context.Context, c Caller, r FactSnapshotRequest) (FactSnapshot, error) {
	return e.service.CreateFactSnapshot(ctx, c, r)
}
func (e *Engine) GetFactSnapshot(ctx context.Context, c Caller, r SnapshotQuery) (FactSnapshot, error) {
	return e.service.GetFactSnapshot(ctx, c, r)
}

type Supersession = model.Supersession
type LifecycleChange = model.LifecycleChange
type HistoryQuery = model.HistoryQuery
type HistoryResult = model.HistoryResult
type FactSnapshotRequest = model.FactSnapshotRequest
type FactSnapshot = model.FactSnapshot
type SnapshotQuery = model.SnapshotQuery

// NewWithAudit injects the bank-owned signer. Export jobs run only on explicit calls.
func NewWithAudit(ledger Ledger, auth Authorizer, evidence EvidenceStore, options model.AuditOptions) (*Engine, error) {
	s, err := services.NewWithAudit(ledger, auth, evidence, options)
	if err != nil {
		return nil, err
	}
	return &Engine{s}, nil
}
func (e *Engine) RecordCalculation(ctx context.Context, c Caller, r CalculationReceipt) (CalculationRecord, error) {
	return e.service.RecordCalculation(ctx, c, r)
}
func (e *Engine) GetCalculationReceipt(ctx context.Context, c Caller, r CalculationQuery) (CalculationRecord, error) {
	return e.service.GetCalculationReceipt(ctx, c, r)
}
func (e *Engine) StartAuditExport(ctx context.Context, c Caller, r AuditExportRequest) (AuditExportResult, error) {
	return e.service.StartAuditExport(ctx, c, r)
}
func (e *Engine) RunAuditExport(ctx context.Context, c Caller, r AuditExportQuery) (AuditExportResult, error) {
	return e.service.RunAuditExport(ctx, c, r)
}
func (e *Engine) GetAuditExport(ctx context.Context, c Caller, r AuditExportQuery) (AuditExportResult, error) {
	return e.service.GetAuditExport(ctx, c, r)
}

type CalculationReceipt = model.CalculationReceipt
type CalculationRecord = model.CalculationRecord
type CalculationQuery = model.CalculationQuery
type AuditExportRequest = model.AuditExportRequest
type AuditExportResult = model.AuditExportResult
type AuditExportQuery = model.AuditExportQuery
type AuditOptions = model.AuditOptions
