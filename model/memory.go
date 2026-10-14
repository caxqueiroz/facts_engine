package model

import "context"

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
