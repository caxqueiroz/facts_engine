package model

import (
	"context"
	"time"
)

type Supersession struct {
	Approval
	ReplacedVersionIDs []string
	Valid              Interval
	Reason             string
	Automated          bool
}

// LifecycleChange applies to an exact accepted version over a bounded interval.
// RejectFact ignores Valid and only allows versions still in proposal state.
type LifecycleChange struct {
	Approval
	Valid  Interval
	Reason string
}
type FactSnapshotRequest struct {
	Command
	Selectors         []FactKey
	ValidAt           time.Time
	KnowledgeRevision int64
}
type SnapshotItem struct {
	Selector                    FactKey
	Resolution                  FactResult
	SchemaVersion, PolicyID     string
	Valid                       Interval
	SourceClasses               []string
	EvidenceIDs, AttestationIDs []string
}
type FactSnapshot struct {
	PayloadDisposed                     bool `json:",omitempty"`
	ID                                  string
	Tenant                              string
	Caller                              Caller
	Purpose                             string
	ValidAt, EvaluatedAt                time.Time
	SelectionRevision, CreationRevision int64
	Items                               []SnapshotItem
	CanonicalVersion, Digest            string
	Authorization                       []AuthorizationDecision
}
type SnapshotQuery struct {
	ID      string
	Purpose string
}
type HistoryQuery struct{ Scope, RecordID, Purpose string }
type HistoryResult struct {
	LedgerRevision int64
	Events         []Event
}

// TemporalTx is implemented by ledgers supporting the second delivery slice.
type TemporalTx interface {
	Tx
	StageTimeline(context.Context, string, []Acceptance) error
	StageRejection(context.Context, string, string) error
	Rejected(context.Context, string, string, int64) (bool, error)
	HasAcceptance(context.Context, string, string) (bool, error)
	Versions(context.Context, string, int64) ([]Version, error)
	HistoricalAttestations(context.Context, string, int64) ([]Attestation, error)
	RevisionTime(context.Context, int64) (time.Time, error)
	StreamRevisionAt(context.Context, string, int64) (int64, error)
	StageSnapshot(context.Context, FactSnapshot) error
	Snapshot(context.Context, string) (FactSnapshot, error)
	History(context.Context, string) ([]Event, error)
}

// SnapshotDigest covers the immutable selection payload. Creation revision and
// authorization receipts are retained separately from this canonical content digest.
func SnapshotDigest(s FactSnapshot) string {
	// Copy before normalization: digesting must not mutate the caller's snapshot.
	s.Items = append([]SnapshotItem(nil), s.Items...)
	for i := range s.Items {
		item := &s.Items[i]
		if item.Valid.From != nil {
			at := item.Valid.From.UTC()
			item.Valid.From = &at
		}
		if item.Valid.To != nil {
			at := item.Valid.To.UTC()
			item.Valid.To = &at
		}
		if item.Resolution.Value != nil {
			value := *item.Resolution.Value
			if value.Instant != nil {
				at := value.Instant.UTC()
				value.Instant = &at
			}
			item.Resolution.Value = &value
		}
	}

	return Digest(struct {
		Version, ID, Tenant  string
		Caller               Caller
		Purpose              string
		ValidAt, EvaluatedAt time.Time
		SelectionRevision    int64
		Items                []SnapshotItem
	}{s.CanonicalVersion, s.ID, s.Tenant, s.Caller, s.Purpose, s.ValidAt.UTC(), s.EvaluatedAt.UTC(), s.SelectionRevision, s.Items})
}
