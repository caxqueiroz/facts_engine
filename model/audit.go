package model

import (
	"context"
	"encoding/json"
	"time"
)

type ArtifactReference struct{ EvidenceID, Digest string }

// CalculationReceipt reports an externally executed run; the engine never executes it.
type CalculationReceipt struct {
	Command
	Scope, SnapshotID, ApplicationID, ExternalRunID                                      string
	MethodID, MethodVersion, ExecutableDigest, ConfigurationDigest, NormalizationVersion string
	ReferenceSnapshotIDs                                                                 []string
	Input, Output                                                                        ArtifactReference
	StartedAt, CompletedAt                                                               time.Time
	Outcome                                                                              string
}
type CalculationRecord struct {
	Authorization                      []AuthorizationDecision
	ID, Tenant, Digest, SnapshotDigest string
	Caller                             Caller
	Receipt                            CalculationReceipt
	LedgerRevision                     int64
}
type CalculationQuery struct{ ID, Purpose string }
type AuditExportRequest struct {
	Command
	Scope           string
	IncludeEvidence bool
}
type AuditExportJob struct {
	ID              string
	Caller          Caller
	Request         AuditExportRequest
	CreatedRevision int64
	State           string
	Fence           int64
	LeaseUntil      *time.Time
}
type AuditExportQuery struct{ ID, Scope, Purpose string }
type AuditEntry struct {
	Kind, ID string
	Payload  json.RawMessage
}
type AuditManifestEntry struct{ Kind, ID, Digest string }
type AuditGap struct{ Kind, ID, Reason string }
type AuditManifest struct {
	SchemaVersion, Encoding, Tenant, Scope, ExportID string
	ThroughRevision                                  int64
	EntryCount                                       int
	Entries                                          []AuditManifestEntry
	Gaps                                             []AuditGap
	Authorization                                    []AuthorizationDecision
}
type AuditSignature struct {
	Algorithm, KeyID string
	Bytes            []byte
}
type AuditBundle struct {
	Manifest  AuditManifest
	Entries   []AuditEntry
	Signature AuditSignature
}
type AuditExportResult struct {
	ID, State                          string
	CreatedRevision, CompletedRevision int64
	Artifact                           ObjectRef
	Bundle                             *AuditBundle
}
type AuditSigner interface {
	Sign(context.Context, []byte) (AuditSignature, error)
}
type AuditOptions struct{ Signer AuditSigner }

// AuditTx keeps business receipts and export completion in the same append transaction.
type AuditTx interface {
	TemporalTx
	CalculationRun(context.Context, string, string) (CalculationRecord, error)
	Calculation(context.Context, string) (CalculationRecord, error)
	StageCalculation(context.Context, CalculationRecord, []FactSnapshot) error
	StageAuditJob(context.Context, AuditExportJob) error
	AuditJob(context.Context, string) (AuditExportJob, error)
	ClaimAuditJob(context.Context, string, time.Duration) (AuditExportJob, error)
	StageAuditBundle(context.Context, AuditExportJob, AuditBundle) error
	AuditExport(context.Context, string) (AuditExportResult, error)
	AuditEntries(context.Context, string) ([]AuditEntry, []AuditGap, error)
}

func CalculationDigest(r CalculationReceipt) string {
	r.Key = ""
	r.CorrelationID = ""
	r.StartedAt = r.StartedAt.UTC()
	r.CompletedAt = r.CompletedAt.UTC()
	return Digest(r)
}
