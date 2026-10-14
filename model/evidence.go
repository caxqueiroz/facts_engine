package model

// EvidenceQuery includes the claimed scope so authorization can precede lookup.
// Object-store credentials are never returned by evidence access operations.
type EvidenceQuery struct{ ID, Scope, Purpose string }
type EvidenceContent struct {
	Metadata Evidence
	Bytes    []byte
}
