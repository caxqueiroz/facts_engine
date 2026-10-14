package model

import "context"

// IDs are application-issued opaque identifiers. Labels never resolve identities.
type ScopeDefinition struct{ ID, Label string }
type SubjectDefinition struct {
	Scope, ID, Kind, Label string
	// Lookup contains qualified, exact external identifiers, not search terms.
	Lookup map[string]string
}
type ScopeRequest struct {
	Command
	Definition       ScopeDefinition
	ExpectedRevision int64
	Reason           string
}
type SubjectRequest struct {
	Command
	Definition       SubjectDefinition
	ExpectedRevision int64
	Reason           string
}
type ScopeVersion struct {
	Definition               ScopeDefinition
	Revision, LedgerRevision int64
	Caller                   Caller
	Reason                   string
}
type SubjectVersion struct {
	Definition               SubjectDefinition
	Revision, LedgerRevision int64
	Caller                   Caller
	Reason                   string
}

// Revision zero selects current metadata; positive revisions select immutable history.
type IdentityQuery struct {
	Scope, ID, Purpose string
	Revision           int64
}
type SubjectLookupQuery struct{ Scope, Namespace, Value, Purpose string }
type IdentityTx interface {
	Tx
	StageScope(context.Context, ScopeVersion, int64) error
	StageSubject(context.Context, SubjectVersion, int64) error
	ScopeIdentity(context.Context, string, int64) (ScopeVersion, error)
	SubjectIdentity(context.Context, string, string, int64) (SubjectVersion, error)
	LookupSubject(context.Context, string, string, string) (SubjectVersion, error)
}
