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
func (e *Engine) RegisterRedactedEvidence(ctx context.Context, c Caller, r model.RedactedEvidenceRequest) (CommitResult, error) {
	return e.service.RegisterRedactedEvidence(ctx, c, r)
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
	ErrQuota        = model.ErrQuota
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

func (e *Engine) ListChanges(ctx context.Context, c Caller, q model.ChangesQuery) (model.ChangesResult, error) {
	return e.service.ListChanges(ctx, c, q)
}

type ChangesQuery = model.ChangesQuery
type ChangesResult = model.ChangesResult

func NewWithRetrieval(l Ledger, a Authorizer, e EvidenceStore, options model.RetrievalOptions) (*Engine, error) {
	s, err := services.NewWithRetrieval(l, a, e, options)
	if err != nil {
		return nil, err
	}
	return &Engine{s}, nil
}
func (e *Engine) Recall(ctx context.Context, c Caller, q model.RecallRequest) (model.RecallResult, error) {
	return e.service.Recall(ctx, c, q)
}

type RecallRequest = model.RecallRequest
type RecallResult = model.RecallResult
type RetrievalOptions = model.RetrievalOptions

func (e *Engine) PrepareContext(ctx context.Context, c Caller, r model.ContextRequest) (model.ContextManifest, error) {
	return e.service.PrepareContext(ctx, c, r)
}
func (e *Engine) GetContextManifest(ctx context.Context, c Caller, q model.ContextQuery) (model.ContextManifest, error) {
	return e.service.GetContextManifest(ctx, c, q)
}
func (e *Engine) RecordDecision(ctx context.Context, c Caller, r model.DecisionReceipt) (model.DecisionRecord, error) {
	return e.service.RecordDecision(ctx, c, r)
}
func (e *Engine) GetDecisionReceipt(ctx context.Context, c Caller, q model.DecisionQuery) (model.DecisionRecord, error) {
	return e.service.GetDecisionReceipt(ctx, c, q)
}

type ContextRequest = model.ContextRequest
type ContextManifest = model.ContextManifest
type ContextQuery = model.ContextQuery
type DecisionReceipt = model.DecisionReceipt
type DecisionRecord = model.DecisionRecord
type DecisionQuery = model.DecisionQuery

func (e *Engine) RegisterRetentionPolicy(ctx context.Context, c Caller, r model.RetentionPolicyRequest) (CommitResult, error) {
	return e.service.RegisterRetentionPolicy(ctx, c, r)
}
func (e *Engine) PlaceLegalHold(ctx context.Context, c Caller, r model.LegalHoldRequest) (CommitResult, error) {
	return e.service.PlaceLegalHold(ctx, c, r)
}
func (e *Engine) ReleaseLegalHold(ctx context.Context, c Caller, r model.ReleaseLegalHoldRequest) (CommitResult, error) {
	return e.service.ReleaseLegalHold(ctx, c, r)
}
func (e *Engine) DisposeEvidence(ctx context.Context, c Caller, r model.EvidenceDisposalRequest) (CommitResult, error) {
	return e.service.DisposeEvidence(ctx, c, r)
}
func (e *Engine) GetRetentionStatus(ctx context.Context, c Caller, q model.RetentionStatusQuery) (model.RetentionStatus, error) {
	return e.service.GetRetentionStatus(ctx, c, q)
}

func (e *Engine) GetEvidenceMetadata(ctx context.Context, c Caller, q model.EvidenceQuery) (model.Evidence, error) {
	return e.service.GetEvidenceMetadata(ctx, c, q)
}
func (e *Engine) GetEvidenceContent(ctx context.Context, c Caller, q model.EvidenceQuery) (model.EvidenceContent, error) {
	return e.service.GetEvidenceContent(ctx, c, q)
}

type EvidenceQuery = model.EvidenceQuery
type EvidenceContent = model.EvidenceContent

func NewWithReads(l Ledger, a Authorizer, e EvidenceStore, options model.ReadOptions) (*Engine, error) {
	s, err := services.NewWithReads(l, a, e, options)
	if err != nil {
		return nil, err
	}
	return &Engine{s}, nil
}
func (e *Engine) ListFacts(ctx context.Context, c Caller, q model.FactsQuery) (model.FactsPage, error) {
	return e.service.ListFacts(ctx, c, q)
}

type FactsQuery = model.FactsQuery
type FactsPage = model.FactsPage
type ReadOptions = model.ReadOptions

func NewWithOptions(l Ledger, a Authorizer, e EvidenceStore, options model.EngineOptions) (*Engine, error) {
	s, err := services.NewWithOptions(l, a, e, options)
	if err != nil {
		return nil, err
	}
	return &Engine{s}, nil
}

type EngineOptions = model.EngineOptions

func (e *Engine) IngestConversation(ctx context.Context, c Caller, r model.ConversationRequest) (model.ConversationRecord, error) {
	return e.service.IngestConversation(ctx, c, r)
}
func (e *Engine) GetConversation(ctx context.Context, c Caller, q model.ConversationQuery) (model.ConversationRecord, error) {
	return e.service.GetConversation(ctx, c, q)
}

type ConversationRequest = model.ConversationRequest
type ConversationRecord = model.ConversationRecord
type ConversationQuery = model.ConversationQuery

func (e *Engine) PlanExtractionChunks(ctx context.Context, c Caller, q model.ExtractionChunkQuery) (model.ExtractionChunkPlan, error) {
	return e.service.PlanExtractionChunks(ctx, c, q)
}

func (e *Engine) StartExtraction(ctx context.Context, c Caller, r model.ExtractionRequest) (model.JobStatus, error) {
	return e.service.StartExtraction(ctx, c, r)
}
func (e *Engine) GetJob(ctx context.Context, c Caller, q model.JobQuery) (model.JobStatus, error) {
	return e.service.GetJob(ctx, c, q)
}
func (e *Engine) RetryJob(ctx context.Context, c Caller, r model.JobRetryRequest) (model.JobStatus, error) {
	return e.service.RetryJob(ctx, c, r)
}

func (e *Engine) RunExtraction(ctx context.Context, c Caller, r model.ExtractionRunRequest) (model.JobStatus, error) {
	return e.service.RunExtraction(ctx, c, r)
}
func (e *Engine) GetExtractionRun(ctx context.Context, c Caller, q model.ExtractionRunQuery) (model.ExtractionRun, error) {
	return e.service.GetExtractionRun(ctx, c, q)
}

func (e *Engine) WaitForProjection(ctx context.Context, c Caller, q model.ProjectionWaitRequest) (model.ProjectionReceipt, error) {
	return e.service.WaitForProjection(ctx, c, q)
}

type ProjectionWaitRequest = model.ProjectionWaitRequest
type ProjectionReceipt = model.ProjectionReceipt

type RevisionConflict = model.RevisionConflict

func (e *Engine) ListFactHistory(ctx context.Context, c Caller, q model.HistoryPageQuery) (model.HistoryPage, error) {
	return e.service.ListFactHistory(ctx, c, q)
}

type HistoryPageQuery = model.HistoryPageQuery
type HistoryPage = model.HistoryPage

func (e *Engine) RegisterMemoryType(ctx context.Context, c Caller, r model.MemoryTypeRequest) (CommitResult, error) {
	return e.service.RegisterMemoryType(ctx, c, r)
}
func (e *Engine) ProposeMemory(ctx context.Context, c Caller, r model.MemoryProposal) (CommitResult, error) {
	return e.service.ProposeMemory(ctx, c, r)
}
func (e *Engine) GetMemoryVersion(ctx context.Context, c Caller, q model.MemoryVersionQuery) (model.MemoryVersion, error) {
	return e.service.GetMemoryVersion(ctx, c, q)
}

type MemoryKey = model.MemoryKey
type MemoryType = model.MemoryType
type MemoryTypeRequest = model.MemoryTypeRequest
type MemoryProposal = model.MemoryProposal
type MemoryVersion = model.MemoryVersion
type MemoryVersionQuery = model.MemoryVersionQuery

func (e *Engine) ApproveMemory(ctx context.Context, c Caller, r Approval) (CommitResult, error) {
	return e.service.ApproveMemory(ctx, c, r)
}
func (e *Engine) AcceptMemory(ctx context.Context, c Caller, r Approval) (CommitResult, error) {
	return e.service.AcceptMemory(ctx, c, r)
}
func (e *Engine) SupersedeMemory(ctx context.Context, c Caller, r Supersession) (CommitResult, error) {
	return e.service.SupersedeMemory(ctx, c, r)
}
func (e *Engine) RejectMemory(ctx context.Context, c Caller, r LifecycleChange) (CommitResult, error) {
	return e.service.RejectMemory(ctx, c, r)
}
func (e *Engine) DisputeMemory(ctx context.Context, c Caller, r LifecycleChange) (CommitResult, error) {
	return e.service.DisputeMemory(ctx, c, r)
}
func (e *Engine) ResolveMemoryDispute(ctx context.Context, c Caller, r LifecycleChange) (CommitResult, error) {
	return e.service.ResolveMemoryDispute(ctx, c, r)
}
func (e *Engine) RetractMemory(ctx context.Context, c Caller, r LifecycleChange) (CommitResult, error) {
	return e.service.RetractMemory(ctx, c, r)
}
func (e *Engine) GetMemory(ctx context.Context, c Caller, q model.MemoryQuery) (model.MemoryResult, error) {
	return e.service.GetMemory(ctx, c, q)
}

type MemoryQuery = model.MemoryQuery
type MemoryResult = model.MemoryResult

func (e *Engine) GetMemoryHistory(ctx context.Context, c Caller, q model.HistoryPageQuery) (model.HistoryPage, error) {
	return e.service.GetMemoryHistory(ctx, c, q)
}

func (e *Engine) RecallMemories(ctx context.Context, c Caller, q model.RecallRequest) (model.MemoryRecallResult, error) {
	return e.service.RecallMemories(ctx, c, q)
}

type MemoryRecallResult = model.MemoryRecallResult

// ListMemories pages through one scope's current memories. It requires an
// engine configured with a pagination key (NewWithReads or EngineOptions.Reads).
func (e *Engine) ListMemories(ctx context.Context, c Caller, q model.MemoriesQuery) (model.MemoriesPage, error) {
	return e.service.ListMemories(ctx, c, q)
}

type MemoriesQuery = model.MemoriesQuery
type MemoriesPage = model.MemoriesPage
type MemoryListItem = model.MemoryListItem

func (e *Engine) StartMemoryRefresh(ctx context.Context, c Caller, r model.MemoryRefreshRequest) (model.JobStatus, error) {
	return e.service.StartMemoryRefresh(ctx, c, r)
}

type MemoryRefreshRequest = model.MemoryRefreshRequest

func (e *Engine) ScanMemoryRefresh(ctx context.Context, c Caller, q model.MemoryRefreshScan) (model.MemoryRefreshScanResult, error) {
	return e.service.ScanMemoryRefresh(ctx, c, q)
}

func (e *Engine) StartMemoryRefreshSweep(ctx context.Context, c Caller, r model.MemoryRefreshSweepRequest) (model.CommitResult, error) {
	return e.service.StartMemoryRefreshSweep(ctx, c, r)
}

// PutScope creates or revises governed scope metadata. OpenFGA provisioning is host-owned.
func (e *Engine) PutScope(ctx context.Context, c Caller, r model.ScopeRequest) (CommitResult, error) {
	return e.service.PutScope(ctx, c, r)
}
func (e *Engine) PutSubject(ctx context.Context, c Caller, r model.SubjectRequest) (CommitResult, error) {
	return e.service.PutSubject(ctx, c, r)
}
func (e *Engine) GetScope(ctx context.Context, c Caller, q model.IdentityQuery) (model.ScopeVersion, error) {
	return e.service.GetScope(ctx, c, q)
}
func (e *Engine) GetSubject(ctx context.Context, c Caller, q model.IdentityQuery) (model.SubjectVersion, error) {
	return e.service.GetSubject(ctx, c, q)
}
func (e *Engine) LookupSubject(ctx context.Context, c Caller, q model.SubjectLookupQuery) (model.SubjectVersion, error) {
	return e.service.LookupSubject(ctx, c, q)
}

type ScopeDefinition = model.ScopeDefinition
type SubjectDefinition = model.SubjectDefinition
type ScopeRequest = model.ScopeRequest
type SubjectRequest = model.SubjectRequest
type ScopeVersion = model.ScopeVersion
type SubjectVersion = model.SubjectVersion
type IdentityQuery = model.IdentityQuery
type SubjectLookupQuery = model.SubjectLookupQuery
type IdentityTx = model.IdentityTx

type ClientConfirmation = model.ClientConfirmation

func (e *Engine) RecordDelegatedAttestation(ctx context.Context, c Caller, r model.DelegatedAttestationRequest) (CommitResult, error) {
	return e.service.RecordDelegatedAttestation(ctx, c, r)
}

type DelegatedAttestationRequest = model.DelegatedAttestationRequest
type AttestationIdentityRequest = model.AttestationIdentityRequest
type AttestationIdentityVerifier = model.AttestationIdentityVerifier
type AttestationIdentityReceipt = model.AttestationIdentityReceipt
type AttestationIdentityOptions = model.AttestationIdentityOptions
type AttestationDelegation = model.AttestationDelegation

type ExtractionCostBudget = model.ExtractionCostBudget
type ExtractionReservation = model.ExtractionReservation
type ExtractionAdmission = model.ExtractionAdmission

// GetProjectionStatus reads tenant-wide operational progress using the host's
// configured operations scope, never a client-selected authorization scope.
func (e *Engine) GetProjectionStatus(ctx context.Context, c Caller, q model.ProjectionStatusQuery) (model.ProjectionStatus, error) {
	return e.service.GetProjectionStatus(ctx, c, q)
}
func (e *Engine) RebuildIndex(ctx context.Context, c Caller, r model.RebuildRequest) (model.RebuildStatus, error) {
	return e.service.RebuildIndex(ctx, c, r)
}

// DisposeOrphanEvidence atomically removes an eligible unregistered upload.
func (e *Engine) DisposeOrphanEvidence(ctx context.Context, c Caller, r model.OrphanEvidenceDisposalRequest) (CommitResult, error) {
	return e.service.DisposeOrphanEvidence(ctx, c, r)
}

// ScanOrphanEvidence returns a bounded page of currently eligible uploads.
func (e *Engine) ScanOrphanEvidence(ctx context.Context, c Caller, q model.OrphanEvidenceQuery) ([]model.ObjectRef, error) {
	return e.service.ScanOrphanEvidence(ctx, c, q)
}

// DisposeSnapshot removes eligible snapshot content while retaining its manifest.
func (e *Engine) DisposeSnapshot(ctx context.Context, c Caller, r model.SnapshotDisposalRequest) (CommitResult, error) {
	return e.service.DisposeSnapshot(ctx, c, r)
}

// DisposeContext removes eligible context content while retaining its manifest.
func (e *Engine) DisposeContext(ctx context.Context, c Caller, r model.ContextDisposalRequest) (CommitResult, error) {
	return e.service.DisposeContext(ctx, c, r)
}

// DisposeAttestation removes eligible fact or memory attestation content while retaining its structural tombstone.
func (e *Engine) DisposeAttestation(ctx context.Context, c Caller, r model.AttestationDisposalRequest) (CommitResult, error) {
	return e.service.DisposeAttestation(ctx, c, r)
}

// DisposeConversation removes retained conversation message references while preserving its audit tombstone.
func (e *Engine) DisposeConversation(ctx context.Context, c Caller, r model.ConversationDisposalRequest) (CommitResult, error) {
	return e.service.DisposeConversation(ctx, c, r)
}

// DisposeExtraction removes eligible terminal-job artifacts and retains provenance.
func (e *Engine) DisposeExtraction(ctx context.Context, c Caller, r model.ExtractionDisposalRequest) (CommitResult, error) {
	return e.service.DisposeExtraction(ctx, c, r)
}

// DisposeVersion removes eligible fact/memory ledger content and records durable
// index cleanup obligations. Its receipt does not imply index cleanup completed.
func (e *Engine) DisposeVersion(ctx context.Context, c Caller, r model.VersionDisposalRequest) (CommitResult, error) {
	return e.service.DisposeVersion(ctx, c, r)
}

// CleanupProjection removes one captured index copy under the original version
// disposal policies and records its acknowledgement. Configure a separately
// provisioned retention destination through EngineOptions.ProjectionCleanup.
func (e *Engine) CleanupProjection(ctx context.Context, c Caller, r model.ProjectionCleanupRequest) (CommitResult, error) {
	return e.service.CleanupProjection(ctx, c, r)
}

// DisposeBatch atomically removes an explicit selection of related fact/memory
// and terminal extraction payloads. Other retained derivatives remain blockers.
func (e *Engine) DisposeBatch(ctx context.Context, c Caller, r model.DisposalBatchRequest) (CommitResult, error) {
	return e.service.DisposeBatch(ctx, c, r)
}
