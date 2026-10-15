package model

import (
	"context"
	"time"
)

// RetentionPolicy is immutable and application-owned. Evidence-byte durations
// start at registration; orphan-evidence durations start at recorded upload time.
// Neither uses a caller-supplied historical source timestamp.
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
type RetentionEvent struct {
	SnapshotDisposal        *SnapshotDisposalRequest `json:",omitempty"`
	AdditionalAuthorization []AuthorizationDecision  `json:",omitempty"`

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
