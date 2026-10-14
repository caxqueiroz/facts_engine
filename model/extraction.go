package model

import (
	"context"
	"encoding/json"
	"time"
)

// ExtractionProvider receives the exact retained request bytes. Implementations
// must honor cancellation and enforce transport response limits. It has no tools
// or ledger mutation capability through this interface.
type ExtractionProvider interface {
	Extract(context.Context, []byte) (ExtractionResponse, error)
}
type ExtractionResponse struct {
	ModelIdentity             string
	Body                      []byte
	InputTokens, OutputTokens int64
}
type ExtractionProfile struct {
	CostUnit                                                                                     string `json:",omitempty"`
	MaxCostUnitsPerAttempt                                                                       int64  `json:",omitempty"`
	ID, ProviderID, RequestedModel, PromptVersion, PromptDigest, ParserVersion, ApplicationBuild string
	Prompt                                                                                       []byte
	Parameters                                                                                   json.RawMessage
	Timeout                                                                                      time.Duration
	MaxInputBytes, MaxOutputBytes, MaxCandidates, MaxAttempts                                    int
	MaxTokensPerAttempt                                                                          int64
	RetryDelay                                                                                   time.Duration
}
type ApprovedExtractor struct {
	Profile  ExtractionProfile
	Provider ExtractionProvider
}
type ExtractionOptions struct {
	CostUnit               string
	DailyCostBudgetUnits   int64
	Profiles               []ApprovedExtractor
	MaxConcurrentPerTenant int
	DailyTokenBudget       int64
}
type PredicateBinding struct{ Predicate, SchemaVersion string }
type ExtractionRequest struct {
	Command
	Scope, ConversationID, ProfileID string
	Subjects                         []string
	Predicates                       []PredicateBinding
	Refresh                          *VersionReference        `json:",omitempty"`
	RefreshTrigger                   *MemoryRefreshTrigger    `json:",omitempty"`
	MemoryTargets                    []MemoryExtractionTarget `json:",omitempty"`
}
type ExtractionJob struct {
	ID, Tenant, ProfileDigest string
	Caller                    Caller
	Request                   ExtractionRequest
	Profile                   ExtractionProfile
	Predicates                []PredicateVersion
	RefreshScopes             []string `json:",omitempty"`
	Authorization             []AuthorizationDecision
	CreatedRevision           int64
	MemoryTypes               []MemoryType `json:",omitempty"`
	DependencyVersions        []Version    `json:",omitempty"`
	ContentScopes             []string     `json:",omitempty"`
}
type JobQuery struct{ ID, Scope, Purpose string }
type JobRetryRequest struct {
	Command
	ID, Scope     string
	ExpectedFence int64
}
type JobStatus struct {
	ID, Scope, Kind, State, AttemptID, LastFailureCode, Purpose string
	Attempts, MaxAttempts                                       int
	Fence, CreatedRevision                                      int64
	AvailableAt                                                 time.Time
	LeaseUntil                                                  *time.Time
}
type ExtractionTx interface {
	Tx
	StageExtractionJob(context.Context, ExtractionJob) error
	ExtractionJob(context.Context, string) (ExtractionJob, error)
	ExtractionJobStatus(context.Context, string) (JobStatus, error)
	RetryExtractionJob(context.Context, string, int64) error
}

func ExtractionProfileDigest(p ExtractionProfile) string { return Digest(p) }

type ExtractionInputMessage struct {
	EvidenceID, Speaker, SpeakerRole, SourceClass string
	SourceTime                                    time.Time
	Content                                       string
}
type ExtractionInput struct {
	Schema, Tenant, JobID, AttemptID, ConversationID, ConversationDigest, Timezone string
	Profile                                                                        ExtractionProfile
	Subjects                                                                       []string
	Predicates                                                                     []PredicateVersion
	Messages                                                                       []ExtractionInputMessage
	OmittedRanges                                                                  []string
	RedactionVersion                                                               string
	Refresh                                                                        *MemoryRefreshInput      `json:",omitempty"`
	MemoryTargets                                                                  []MemoryExtractionTarget `json:",omitempty"`
	MemoryTypes                                                                    []MemoryType             `json:",omitempty"`
	DependencyVersions                                                             []Version                `json:",omitempty"`
}
type ExtractionCandidate struct {
	Subject, Predicate, Discriminator, SchemaVersion string
	Value                                            Value
	Valid                                            Interval
	Sources                                          []SourceSpan
	Uncertainty, Rationale                           string
	Confidence                                       float64
}
type ExtractionOutput struct {
	Candidates []ExtractionCandidate `json:"candidates"`
}
type ExtractionIssue struct {
	Candidate int
	Code      string
}
type ExtractionRelation struct {
	Candidate                 int
	RecordID, VersionID, Kind string
}
type ExtractedProposal struct {
	RecordKind          string `json:",omitempty"`
	Candidate           int
	RecordID, VersionID string
}
type ExtractionAttempt struct {
	Reservation                                                        *ExtractionReservation `json:",omitempty"`
	ContentScopes                                                      []string               `json:",omitempty"`
	RefreshScopes                                                      []string               `json:",omitempty"`
	ID, Tenant, Scope, JobID, OwnerNonce, ProfileDigest, RequestDigest string
	Caller                                                             Caller
	Fence, LedgerRevision                                              int64
	LeaseUntil                                                         time.Time
	Number                                                             int
	StartedAt                                                          time.Time
	RequestBytes                                                       []byte
	Authorization                                                      []AuthorizationDecision
}
type ExtractionResult struct {
	ContentScopes                                                                          []string `json:",omitempty"`
	RefreshScopes                                                                          []string `json:",omitempty"`
	AttemptID, JobID, Tenant, Scope, Outcome, ModelIdentity, RequestDigest, ResponseDigest string
	ResponseBytes                                                                          []byte
	ResponseComplete                                                                       bool
	InputTokens, OutputTokens                                                              int64
	Issues                                                                                 []ExtractionIssue
	Relations                                                                              []ExtractionRelation
	Proposals                                                                              []ExtractedProposal
	Authorization                                                                          []AuthorizationDecision
	CompletedAt                                                                            time.Time
	LedgerRevision                                                                         int64
}
type ExtractionRunRequest struct {
	Command
	ID, Scope string
}
type ExtractionRunQuery struct{ ID, Scope, Purpose string }
type ExtractionRun struct {
	Attempt ExtractionAttempt
	Result  *ExtractionResult
}
type ExtractionRunTx interface {
	ExtractionTx
	TemporalTx
	BeginExtraction(context.Context, string, string, int, int64, int64, time.Duration, ExtractionCostBudget) (ExtractionAdmission, error)
	FailExpiredExtraction(context.Context, string, int64) error
	RejectExtractionInput(context.Context, string) error
	StageExtractionAttempt(context.Context, ExtractionAttempt) error
	ExtractionAttempt(context.Context, string) (ExtractionAttempt, error)
	ExtractionResult(context.Context, string) (ExtractionResult, error)
	FinishExtraction(context.Context, string, string, int64, string, string, time.Duration) error
	StageExtractionResult(context.Context, ExtractionResult) error
}

// ExtractionRunner is the authorized engine boundary used by explicit workers.
type ExtractionRunner interface {
	GetJob(context.Context, Caller, JobQuery) (JobStatus, error)
	RunExtraction(context.Context, Caller, ExtractionRunRequest) (JobStatus, error)
}

func ExtractionConsumer(stream string) string { return "extraction-" + Digest(stream) }

// MemoryRefreshRequest binds an asynchronous replacement proposal to an exact
// accepted memory version. The job never accepts or supersedes it automatically.
type MemoryRefreshRequest struct {
	Trigger *MemoryRefreshTrigger `json:",omitempty"`
	Command
	Scope, RecordID, VersionID, ProfileID string
}
type MemoryRefreshInput struct {
	Schema                         string
	Original                       MemoryVersion
	Definition                     MemoryType
	LedgerRevision, StreamRevision int64
	Dependencies                   []MemoryRefreshDependency
	Evidence                       []ExtractionInputMessage
}
type MemoryRefreshDependency struct {
	Scope, RecordID string
	Timeline        []Acceptance
	Versions        []Version
}
type MemoryRefreshOutput struct {
	Body          string
	Dependencies  []MemoryDependency
	DeclineReason string
}

type MemoryExtractionTarget struct {
	ID, Subtype, SchemaVersion string
	Dependencies               []MemoryDependency
}
type MemoryExtractionCandidate struct {
	ID, Subtype, SchemaVersion, Body string
	Valid                            Interval
	Sources                          []SourceSpan
	Uncertainty, Rationale           string
	Confidence                       float64
}
type MemoryExtractionOutput struct {
	Candidates []MemoryExtractionCandidate `json:"candidates"`
}
type MemoryExtractionTx interface {
	MemoryLifecycleTx
	MemoryVersions(context.Context, string, int64) ([]MemoryVersion, error)
}
