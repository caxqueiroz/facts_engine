package model

import (
	"context"
	"time"
)

// MemoryKey is an explicit application-issued identity, independent of fact keys.
type MemoryKey struct{ Scope, ID string }
type MemoryType struct {
	Scope, Subtype, Version, PolicyID, Classification string
	Migration                                         *PredicateMigration `json:",omitempty"`
}
type MemoryTypeRequest struct {
	Command
	Definition MemoryType
}
type MemoryDependency struct {
	Scope, RecordID, VersionID, ContentDigest string
	// Label preserves the application's distinction between assertions and uncertainty.
	Label string
}
type MemoryProposal struct {
	Command
	Key                          MemoryKey
	Subtype, SchemaVersion, Body string
	Valid                        Interval
	Sources                      []SourceSpan
	Dependencies                 []MemoryDependency
	ExpectedRevision             int64
}
type MemoryVersion struct {
	PayloadDisposed                bool `json:",omitempty"`
	Authorization                  []AuthorizationDecision
	ID, RecordID, Digest, PolicyID string
	Proposal                       MemoryProposal
	Proposer                       Caller
	LedgerRevision                 int64
}
type MemoryVersionQuery struct{ Scope, RecordID, VersionID, Purpose string }
type MemoryTx interface {
	Tx
	MemoryType(context.Context, string, string, string) (MemoryType, error)
	ActiveMemoryType(context.Context, string, string) (string, error)
	StageMemoryType(context.Context, MemoryType) error
	LockMemory(context.Context, MemoryKey) (Record, error)
	StageMemoryVersion(context.Context, MemoryVersion) error
	MemoryVersion(context.Context, string, string) (MemoryVersion, error)
}

func MemoryDigest(p MemoryProposal) string {
	return Digest(struct {
		Key                          MemoryKey
		Subtype, SchemaVersion, Body string
		Valid                        Interval
		Sources                      []SourceSpan
		Dependencies                 []MemoryDependency
	}{p.Key, p.Subtype, p.SchemaVersion, p.Body, p.Valid, p.Sources, p.Dependencies})
}

type MemoryQuery struct {
	Key               MemoryKey
	Purpose           string
	ValidAt           time.Time
	KnowledgeRevision int64
	KnownAt           *time.Time
}
type MemoryResult struct {
	State, RecordID, VersionID, Body string
	StreamRevision, LedgerRevision   int64
	Dependencies                     []MemoryDependency
}
type MemoryLifecycleTx interface {
	MemoryTx
	TemporalTx
	MemoryTimeline(context.Context, string, int64) ([]Acceptance, error)
	StageMemoryTimeline(context.Context, string, []Acceptance) error
	MemoryRejected(context.Context, string, string, int64) (bool, error)
	MemoryHasAcceptance(context.Context, string, string) (bool, error)
	StageMemoryRejection(context.Context, string, string) error
	MemoryStreamRevisionAt(context.Context, string, int64) (int64, error)
	MemoryAttestations(context.Context, string, int64) ([]Attestation, error)
	StageMemoryAttestation(context.Context, Attestation) error
}

func MemoryRecordID(k MemoryKey) string {
	return Digest(struct {
		Kind string
		Key  MemoryKey
	}{"memory", k})
}
