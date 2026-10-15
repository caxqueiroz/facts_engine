package model

import (
	"context"
	"slices"
	"time"
)

type RetrievalIdentity struct {
	Generation, EmbeddingID, SearchVersion, RerankerID string
	Dimensions                                         int
}
type RetrievalDescriber interface {
	DescribeRetrieval(context.Context, string, string) (RetrievalIdentity, error)
}
type ContextRequest struct {
	Command
	Scope           string
	Query           RecallRequest
	IncludeMemories bool `json:",omitempty"`
}

// ContextArchiveOptions is trusted host policy for all context deliveries on an
// engine. Progress must read the configured archive consumer's ledger domain.
type ContextArchiveOptions struct {
	Stream              string
	MaxPendingRevisions int64
	Progress            OutboxProgressReader
}

// ContextArchiveReceipt records the preparation-time policy check. It is not a
// signed archive-head proof; current delivery is rechecked independently.
type ContextArchiveReceipt struct {
	Stream                                                    string
	MaxPendingRevisions, LedgerRevision, AcknowledgedRevision int64
	CheckedAt                                                 time.Time
}

type ContextManifest struct {
	PayloadDisposed                                      bool                   `json:",omitempty"`
	Archive                                              *ContextArchiveReceipt `json:",omitempty"`
	ID, Tenant, Scope                                    string
	Caller                                               Caller
	Request                                              ContextRequest
	CanonicalVersion, RedactionVersion, QueryReference   string
	Retrieval                                            RetrievalIdentity
	Items                                                []RecallItem
	Memories                                             []MemoryRecallItem `json:",omitempty"`
	Order                                                []VersionReference `json:",omitempty"`
	Rendered                                             []byte
	RenderedDigest, Digest                               string
	SelectionRevision, IndexedRevision, CreationRevision int64
	Incomplete                                           bool
	Authorization                                        []AuthorizationDecision
}
type ContextQuery struct{ ID, Purpose string }

func ContextDigest(m ContextManifest) string {
	m.PayloadDisposed = false
	m.ID = ""
	m.Digest = ""
	m.CreationRevision = 0
	m.Authorization = nil
	m.Archive = nil
	m.Request.Key = ""
	m.Request.CorrelationID = ""
	m.Request.Query.ValidAt = m.Request.Query.ValidAt.UTC()
	return Digest(m)
}

type VersionReference struct{ RecordID, VersionID string }
type DecisionReceipt struct {
	Command
	Scope, ApplicationID, ExternalDecisionID                   string
	ContextID, SnapshotID, CalculationID                       string
	SelectedItems, UsedItems                                   []VersionReference
	MethodKind, MethodID, MethodVersion, TransformationVersion string
	Input, Output                                              ArtifactReference
	ModelRequest, ModelResponse, ActionReceipt                 *ArtifactReference
	DecidedAt                                                  time.Time
	Outcome                                                    string
}
type DecisionRecord struct {
	PayloadDisposed                                     bool `json:",omitempty"`
	ID, Tenant, Digest, SourceDigest, CalculationDigest string
	Caller                                              Caller
	Receipt                                             DecisionReceipt
	Scopes                                              []string
	LedgerRevision                                      int64
	Authorization                                       []AuthorizationDecision
}
type DecisionQuery struct{ ID, Purpose string }

func DecisionDigest(r DecisionReceipt) string {
	r.Key = ""
	r.CorrelationID = ""
	r.DecidedAt = r.DecidedAt.UTC()
	return Digest(r)
}
func DecisionArtifacts(r DecisionReceipt) map[string]ArtifactReference {
	refs := map[string]ArtifactReference{"input": r.Input, "output": r.Output}
	if r.ModelRequest != nil {
		refs["model_request"] = *r.ModelRequest
	}
	if r.ModelResponse != nil {
		refs["model_response"] = *r.ModelResponse
	}
	if r.ActionReceipt != nil {
		refs["action_receipt"] = *r.ActionReceipt
	}
	return refs
}

type ContextTx interface {
	AuditTx
	ContextManifest(context.Context, string) (ContextManifest, error)
	StageContext(context.Context, ContextManifest) error
	Decision(context.Context, string) (DecisionRecord, error)
	DecisionRun(context.Context, string, string) (DecisionRecord, error)
	StageDecision(context.Context, DecisionRecord, map[VersionReference]string) error
}

// ContextScopes includes every scope whose content is retained in the manifest.
func ContextScopes(m ContextManifest) []string {
	scopes := append([]string{}, m.Request.Query.Scopes...)
	for _, item := range m.Memories {
		scopes = append(scopes, item.Key.Scope)
		for _, dependency := range item.Memory.Dependencies {
			scopes = append(scopes, dependency.Scope)
		}
	}
	slices.Sort(scopes)
	return slices.Compact(scopes)
}

// ContextTombstone removes copied content while retaining the original digests,
// version inventory, scope boundaries and authorization provenance. It does not
// authorize disposal or validate its input. The returned metadata cannot be used
// to reconstruct or verify the original context content. Input is not mutated.
func ContextTombstone(m ContextManifest) ContextManifest {
	m.PayloadDisposed = true
	m.Rendered = nil
	m.Request.Query.Query = ""
	m.Items = slices.Clone(m.Items)
	for i := range m.Items {
		m.Items[i].Fact.Value = nil
	}
	m.Memories = slices.Clone(m.Memories)
	for i := range m.Memories {
		memory := &m.Memories[i].Memory
		memory.Body = ""
		memory.Dependencies = slices.Clone(memory.Dependencies)
		for j := range memory.Dependencies {
			memory.Dependencies[j].Label = ""
		}
	}
	return m
}
