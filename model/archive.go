package model

import "context"

// ArchiveStore is separately administered immutable object storage. Put is
// create-once by logical key, returning the same version on identical retries.
// Digest covers plaintext; Version identifies immutable encrypted stored bytes.
type ArchiveStore interface {
	Put(context.Context, string, string, []byte) (ObjectRef, error)
	Load(context.Context, string, string) (ObjectRef, []byte, error)
	Read(context.Context, string, ObjectRef) ([]byte, error)
}
type ArchivePayload struct {
	Scopes  []string
	Event   Event
	Entries []AuditEntry
	Gaps    []AuditGap
}
type ArchiveSource interface {
	ArchivePayload(context.Context, Delivery) (ArchivePayload, error)
}
type ArchiveManifest struct {
	Authorization                    []AuthorizationDecision
	Schema, Encoding, Tenant, Stream string
	FromRevision, ThroughRevision    int64
	EventCount                       int
	Payload                          ObjectRef
	Previous                         *ObjectRef
}
type ArchiveBatch struct {
	Manifest  ArchiveManifest
	Signature AuditSignature
}

func ArchiveConsumer(stream string) string { return "archive-" + Digest(stream) }

// EvidenceDisposalConsumer identifies the trusted consumer that performs
// post-commit deletion from external immutable evidence stores.
func EvidenceDisposalConsumer() string { return "evidence-disposal" }
