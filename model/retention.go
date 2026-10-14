package model

import (
	"context"
	"time"
)

// RetentionPolicy is immutable and application-owned. Durations are measured from
// evidence registration, never a caller-supplied historical source timestamp.
type RetentionPolicy struct {
	ID, Scope, RecordClass, Purpose, Jurisdiction string
	MinimumAgeSeconds                             int64
}
type RetentionPolicyRequest struct {
	Command
	Policy RetentionPolicy
}
type LegalHoldRequest struct {
	Command
	ID, Scope, Reason string
}
type ReleaseLegalHoldRequest struct {
	Command
	ID, Scope, Reason string
}
type EvidenceDisposalRequest struct {
	Command
	Scope, EvidenceID, PolicyID, Jurisdiction, Reason string
}
type RetentionEvent struct {
	ID, Tenant, Scope, Kind, TargetID string
	Caller                            Caller
	Command                           Command
	Policy                            *RetentionPolicy
	Hold                              *LegalHoldRequest
	Release                           *ReleaseLegalHoldRequest
	Disposal                          *EvidenceDisposalRequest
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
