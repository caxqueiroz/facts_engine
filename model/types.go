// Package model defines the engine's transport-independent contracts.
package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalid      = errors.New("invalid input")
	ErrDenied       = errors.New("permission denied")
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("command identity conflict")
	ErrRevision     = errors.New("stream revision conflict")
	ErrPrecondition = errors.New("precondition failed")
	ErrUnavailable  = errors.New("dependency unavailable")
)

type Caller struct {
	Tenant, Principal, Initiator string
	Service                      bool
}

func (c Caller) Human() string {
	if c.Initiator != "" {
		return c.Initiator
	}
	return c.Principal
}

type Command struct{ Key, Purpose, CorrelationID string }
type CommitResult struct {
	ContentDigest                  string
	ID, RecordID, VersionID        string
	StreamRevision, LedgerRevision int64
}
type FactKey struct{ Scope, Subject, Predicate, Discriminator string }
type Interval struct {
	From *time.Time
	To   *time.Time
}

func (i Interval) Validate() error {
	// PostgreSQL ranges have microsecond precision. Reject lossy boundaries.
	for _, at := range []*time.Time{i.From, i.To} {
		if at != nil && (at.IsZero() || at.Nanosecond()%1000 != 0) {
			return ErrInvalid
		}
	}
	if i.From != nil && i.To != nil && !i.From.Before(*i.To) {
		return ErrInvalid
	}
	return nil
}
func (i Interval) Contains(t time.Time) bool {
	return i.From != nil && !t.Before(*i.From) && (i.To == nil || t.Before(*i.To))
}
func (i Interval) Overlaps(j Interval) bool {
	return (i.To == nil || j.From == nil || j.From.Before(*i.To)) && (j.To == nil || i.From == nil || i.From.Before(*j.To))
}

type ValueKind string

const (
	Text    ValueKind = "text"
	Boolean ValueKind = "boolean"
	Integer ValueKind = "integer"
	Decimal ValueKind = "decimal"
	Date    ValueKind = "date"
	Instant ValueKind = "instant"
	Entity  ValueKind = "entity"
	Unknown ValueKind = "unknown"
)

// Value has exactly one payload. Unknown has no payload. Decimal strings are canonical.
type Value struct {
	Kind    ValueKind
	Text    *string
	Boolean *bool
	Integer *int64
	Decimal *string
	Unit    string
	Date    *string
	Instant *time.Time
	Entity  *string
}

var decimalRE = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]*[1-9])?$`)
var opaqueRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func ValidID(s string) bool { return opaqueRE.MatchString(s) }
func (v Value) Validate() error {
	n := 0
	for _, ok := range []bool{v.Text != nil, v.Boolean != nil, v.Integer != nil, v.Decimal != nil, v.Date != nil, v.Instant != nil, v.Entity != nil} {
		if ok {
			n++
		}
	}
	if v.Kind == Unknown {
		if n == 0 && v.Unit == "" {
			return nil
		}
		return ErrInvalid
	}
	if n != 1 || (v.Kind != Decimal && v.Unit != "") {
		return ErrInvalid
	}
	switch v.Kind {
	case Text:
		if v.Text != nil && utf8.ValidString(*v.Text) && len(*v.Text) <= 65536 {
			return nil
		}
	case Boolean:
		if v.Boolean != nil {
			return nil
		}
	case Integer:
		if v.Integer != nil {
			return nil
		}
	case Decimal:
		if v.Decimal != nil && len(*v.Decimal) <= 128 && decimalRE.MatchString(*v.Decimal) && *v.Decimal != "-0" && len(v.Unit) <= 32 {
			return nil
		}
	case Date:
		if v.Date != nil {
			if _, err := time.Parse("2006-01-02", *v.Date); err == nil {
				return nil
			}
		}
	case Instant:
		if v.Instant != nil && !v.Instant.IsZero() {
			return nil
		}
	case Entity:
		if v.Entity != nil && ValidID(*v.Entity) {
			return nil
		}
	}
	return ErrInvalid
}
func Digest(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("non-serializable domain type: %v", err))
	}
	return BytesDigest(b)
}
func BytesDigest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

type PredicateVersion struct {
	Scope, Predicate, Version, PolicyID string
	Kind                                ValueKind
	Classification                      string
	Constraints                         *ValueConstraints `json:",omitempty"`
	Cardinality                         string            `json:",omitempty"`
	TemporalPrecision                   string            `json:",omitempty"`
}
type AttestationRequirement struct {
	Kind       string
	Principals []string
}
type AcceptancePolicy struct {
	Scope, ID                        string
	Mode                             string
	AcceptingServices, SourceClasses []string
	Required                         []AttestationRequirement
}
type PredicateRequest struct {
	Command
	Definition PredicateVersion
}
type PolicyRequest struct {
	Command
	Policy AcceptancePolicy
}
type ObjectRef struct {
	Locator, Version, Digest string
	Size                     int64
}
type Evidence struct {
	ID, Scope, SourceSystem, SourceObject, SourceRevision, Speaker, SourceClass, MediaType, Classification, RetentionPolicy string
	SourceTime                                                                                                              time.Time
	Object                                                                                                                  ObjectRef
}
type EvidenceRequest struct {
	Command
	Evidence Evidence
}
type SourceSpan struct {
	EvidenceID string
	Start, End int
	Quote      string
}
type FactProposal struct {
	Command
	Key              FactKey
	SchemaVersion    string
	Value            Value
	Valid            Interval
	Sources          []SourceSpan
	ExpectedRevision int64
}
type Version struct {
	ExtractionAttemptID  string `json:",omitempty"`
	ID, RecordID, Digest string
	Proposal             FactProposal
	Proposer             Caller
	PolicyID             string
}
type Record struct {
	ID       string
	Key      FactKey
	Revision int64
}
type Approval struct {
	Command
	Scope, RecordID, VersionID string
	ExpectedRevision           int64
}
type Attestation struct {
	ID, Scope, RecordID, VersionID, ContentDigest, Kind, EvidenceID string
	Attestor                                                        Caller
	ExpiresAt                                                       *time.Time
}
type AttestationRequest struct {
	Command
	Attestation Attestation
}
type Withdrawal struct {
	Command
	Scope, AttestationID string
}
type Acceptance struct {
	State          string
	VersionID      string
	Valid          Interval
	PolicyID       string
	AttestationIDs []string
	LedgerRevision int64
}
type FactQuery struct {
	KnownAt           *time.Time
	Purpose           string
	Key               FactKey
	ValidAt           time.Time
	KnowledgeRevision int64
}
type FactResult struct {
	State                          string
	RecordID, VersionID            string
	Value                          *Value
	LedgerRevision, StreamRevision int64
}
type Event struct {
	DefinitionPredicate                  string `json:",omitempty"`
	EntityID, Reason                     string
	Affected                             *Interval
	ReplacedVersionIDs                   []string
	ID, Type, Scope, RecordID, VersionID string
	StreamRevision, LedgerRevision       int64
	Caller                               Caller
	Command                              Command
	Authorization                        AuthorizationDecision
	RecordedAt                           time.Time
}
type AuthorizationRequest struct {
	Caller                                    Caller
	Scope, Permission, Purpose, CorrelationID string
}
type AuthorizationDecision struct {
	Allowed                             bool
	ModelID, ContextDigest, Consistency string
	CheckedAt                           time.Time
	Request                             AuthorizationRequest
}
type Authorizer interface {
	Check(context.Context, AuthorizationRequest) (AuthorizationDecision, error)
}

// EvidenceStore reads only immutable references. Implementations must enforce tenant isolation.
type EvidenceStore interface {
	Read(context.Context, string, ObjectRef) ([]byte, error)
}

// Ledger owns transaction boundaries. Callbacks must not perform network calls.
type Ledger interface {
	Transact(context.Context, string, func(Tx) error) error
}
type Tx interface {
	Receipt(context.Context, Caller, string, string, string) (*CommitResult, error)
	SaveReceipt(context.Context, Caller, string, string, string, CommitResult) error
	Policy(context.Context, string, string) (AcceptancePolicy, error)
	InsertPolicy(context.Context, AcceptancePolicy) error
	Predicate(context.Context, string, string, string) (PredicateVersion, error)
	InsertPredicate(context.Context, PredicateVersion) error
	ActivePredicate(context.Context, string, string) (string, error)
	Evidence(context.Context, string) (Evidence, error)
	InsertEvidence(context.Context, Evidence) (string, error)
	LockRecord(context.Context, FactKey) (Record, error)
	Record(context.Context, string) (Record, error)
	SetRevision(context.Context, string, int64) error
	Version(context.Context, string, string) (Version, error)
	InsertVersion(context.Context, Version) error
	Attestations(context.Context, string) ([]Attestation, error)
	InsertAttestation(context.Context, Attestation) error
	WithdrawAttestation(context.Context, string, string, Caller) (CommitResult, error)
	Acceptances(context.Context, string, int64) ([]Acceptance, error)
	InsertAcceptance(context.Context, string, Acceptance) error
	Append(context.Context, Event) (Event, error)
	Revision(context.Context) (int64, error)
}
