package model

import (
	"context"
	"time"
)

// RetentionPolicy is immutable and application-owned. Evidence-byte durations
// start at registration; orphan-evidence durations start at recorded upload time.
// Snapshot/context and extraction artifact durations start at their database
// creation event times; each extraction payload class has its own policy.
// None uses a caller-supplied historical source timestamp.
type RetentionPolicy struct {
	ID, Scope, RecordClass, Purpose, Jurisdiction string
	MinimumAgeSeconds                             int64
}
type RetentionPolicyRequest struct {
	Command
	Policy RetentionPolicy
}

// PendingEvidenceUpload binds a new development-store object to its intended
// scope and orphan cleanup policy before ledger registration. It grants no
// permission to disclose content or to register evidence.
type PendingEvidenceUpload struct{ ID, Scope, PolicyID string }
type LegalHoldRequest struct {
	Command
	ID, Scope, Reason string
}
type ReleaseLegalHoldRequest struct {
	Command
	ID, Scope, Reason string
}
type EvidenceDisposalRequest struct {
	// DerivativeEvidenceIDs explicitly selects the complete direct derivative set.
	// Empty preserves single-object disposal; custom adapters must implement EvidenceLineageDisposalTx.
	DerivativeEvidenceIDs []string `json:",omitempty"`
	Command
	Scope, EvidenceID, PolicyID, Jurisdiction, Reason string
}

// OrphanEvidenceDisposalRequest selects exactly one immutable upload version.
// It can only delete an object that has never been registered as evidence.
type OrphanEvidenceDisposalRequest struct {
	Command
	Scope, PolicyID, Jurisdiction, Reason string
	Object                                ObjectRef
}
type OrphanEvidenceDisposalTx interface {
	DisposeOrphanEvidence(context.Context, OrphanEvidenceDisposalRequest) error
}
type SnapshotRetentionBinding struct{ Scope, PolicyID, Jurisdiction string }
type SnapshotDisposalRequest struct {
	Command
	SnapshotID, Reason string
	Bindings           []SnapshotRetentionBinding
}

// SnapshotDisposalTx adapters must serialize disposal with new references,
// scope holds, exports and archive reads, and retain an immutable tombstone.
type SnapshotDisposalTx interface {
	DisposeSnapshot(context.Context, SnapshotDisposalRequest) error
}
type ContextRetentionBinding struct{ Scope, PolicyID, Jurisdiction string }
type ContextDisposalRequest struct {
	Command
	ContextID, Reason string
	Bindings          []ContextRetentionBinding
}

// ContextDisposalTx adapters must serialize disposal with new references,
// scope holds, exports and archive reads, and retain an immutable tombstone.
type ContextDisposalTx interface {
	DisposeContext(context.Context, ContextDisposalRequest) error
}

// ExtractionRetentionBinding selects independently governed scheduling, request,
// and response payload policies for one content scope.
type ExtractionRetentionBinding struct {
	Scope, JobPolicyID, RequestPolicyID, ResponsePolicyID, Jurisdiction string
}

// ExtractionDisposalRequest selects a terminal job and all of its retained
// attempts/results, including attempts from earlier retries.
type ExtractionDisposalRequest struct {
	Command
	JobID, Reason string
	Bindings      []ExtractionRetentionBinding
}

// ExtractionDisposalTx must atomically verify terminal state, complete content
// scope/input inventories, class-specific policy ages, holds, live derivatives,
// exports and archive progress before retaining tombstones and deleting payloads.
// Implementations must serialize with execution, retries and new references.
// This interface grants no authority to dispose source facts, memories or evidence.
type ExtractionDisposalTx interface {
	DisposeExtraction(context.Context, ExtractionDisposalRequest) error
}

type VersionRetentionBinding struct{ Scope, PolicyID, Jurisdiction string }

// VersionDisposalRequest selects one immutable fact or memory payload. Its
// index cleanup obligations are durable but may remain pending after commit.
type VersionDisposalRequest struct {
	Command
	Kind, Scope, RecordID, VersionID, Reason string
	Bindings                                 []VersionRetentionBinding
}

// VersionDisposalTx atomically validates retention policy and retained copies,
// fences references/lifecycle writes, retains a tombstone, captures index cleanup
// obligations and increments the owning record revision. It returns that revision.
type VersionDisposalTx interface {
	DisposeVersion(context.Context, VersionDisposalRequest) (int64, error)
}

type RetentionEvent struct {
	CalculationDisposal     *CalculationDisposalRequest `json:",omitempty"`
	BatchID                 string                      `json:",omitempty"`
	DisposalBatch           *DisposalBatchRequest       `json:",omitempty"`
	ProjectionCleanup       *ProjectionCleanupRequest   `json:",omitempty"`
	VersionDisposal         *VersionDisposalRequest     `json:",omitempty"`
	ExtractionDisposal      *ExtractionDisposalRequest  `json:",omitempty"`
	ContextDisposal         *ContextDisposalRequest     `json:",omitempty"`
	SnapshotDisposal        *SnapshotDisposalRequest    `json:",omitempty"`
	AdditionalAuthorization []AuthorizationDecision     `json:",omitempty"`

	ID, Tenant, Scope, Kind, TargetID string
	Caller                            Caller
	Command                           Command
	Policy                            *RetentionPolicy
	Hold                              *LegalHoldRequest
	Release                           *ReleaseLegalHoldRequest
	Disposal                          *EvidenceDisposalRequest
	OrphanDisposal                    *OrphanEvidenceDisposalRequest `json:",omitempty"`
	LedgerRevision                    int64
	Limitations                       []string
}
type RetentionStatusQuery struct{ Scope, EvidenceID, Purpose string }
type RetentionStatus struct {
	EvidenceID, PolicyID string
	RegisteredAt         time.Time
	Disposed             bool
	ActiveHolds          []string
	Blockers             []string
}

// RetentionDomain binds the development disposal operation to one database.
// Production object-store disposal requires a separate coordinated adapter.
type RetentionDomain interface {
	RetentionDomain(context.Context) (string, error)
}
type RetentionTx interface {
	Tx
	RetentionPolicy(context.Context, string, string) (RetentionPolicy, error)
	StageRetention(context.Context, RetentionEvent) error
	RetentionStatus(context.Context, string, string) (RetentionStatus, error)
	DisposeEvidence(context.Context, EvidenceDisposalRequest) error
}

// EvidenceLineageDisposalTx atomically disposes an original and its explicitly
// enumerated derivatives under the same retention policy and scope.
type EvidenceLineageDisposalTx interface {
	DisposeEvidenceLineage(context.Context, EvidenceDisposalRequest) error
}

// OrphanEvidenceQuery is a bounded, live scan, not a deletion authorization.
// The optional cursor is the last locator/version pair returned by a prior page.
type OrphanEvidenceQuery struct {
	Scope, PolicyID, Jurisdiction, Purpose string
	AfterLocator, AfterVersion             string
	Limit                                  int
}
type OrphanEvidenceScannerTx interface {
	ScanOrphanEvidence(context.Context, OrphanEvidenceQuery) ([]ObjectRef, error)
}
type OrphanEvidenceEngine interface {
	ScanOrphanEvidence(context.Context, Caller, OrphanEvidenceQuery) ([]ObjectRef, error)
	DisposeOrphanEvidence(context.Context, Caller, OrphanEvidenceDisposalRequest) (CommitResult, error)
}

// CalculationDisposalRequest selects independently governed scope policies.
type CalculationDisposalRequest struct {
	Command
	CalculationID, Reason string
	Bindings              []CalculationRetentionBinding
}
type CalculationRetentionBinding struct{ Scope, PolicyID, Jurisdiction string }
