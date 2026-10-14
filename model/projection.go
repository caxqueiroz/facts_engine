package model

import "context"
import "time"

// ProjectionDocument contains canonical accepted fact or memory content, never source-span quotes.
type ProjectionDocument struct {
	Memory                                                                           *MemoryProjection `json:",omitempty"`
	Tenant, Generation, RecordID, VersionID, VersionDigest, PolicyID, Classification string
	Key                                                                              FactKey
	Value                                                                            Value
	Valid                                                                            Interval
	SourceClasses                                                                    []string
}

type MemoryProjection struct {
	Key                          MemoryKey
	Subtype, SchemaVersion, Body string
	Dependencies                 []MemoryDependency
}

func (d ProjectionDocument) Scope() string {
	if d.Memory != nil {
		return d.Memory.Key.Scope
	}
	return d.Key.Scope
}
func ProjectionID(d ProjectionDocument) string {
	if d.Memory != nil {
		return Digest([]string{"memory-projection-v1", d.Tenant, d.Generation, d.VersionID})
	}
	return Digest([]string{"facts-projection-v1", d.Tenant, d.Generation, d.VersionID})
}
func ProjectionConsumer(generation string) string { return "projection-" + Digest(generation) }

type ProjectionSource interface {
	ProjectionDocument(context.Context, Delivery, string) (*ProjectionDocument, error)
}
type SearchCandidate struct {
	Kind                string `json:",omitempty"`
	RecordID, VersionID string
	Score               float64
}
type RetrievalIndex interface {
	Put(context.Context, ProjectionDocument) error
	Candidates(context.Context, string, string, string, string, int) ([]SearchCandidate, error)
}
type Embedder interface {
	Embed(context.Context, string) ([]float32, error)
}
type RetrievalOptions struct {
	Registry   ProjectionRegistry
	Index      RetrievalIndex
	Progress   Outbox
	Generation string
}
type RecallRequest struct {
	Scopes                             []string
	Query, Purpose                     string
	RequireGeneration                  string `json:",omitempty"`
	ValidAt                            time.Time
	KnowledgeRevision, RequireRevision int64
	Limit                              int
}
type RecallItem struct {
	Key   FactKey
	Fact  FactResult
	Score float64
}
type RecallResult struct {
	Items                           []RecallItem
	Generation                      string
	LedgerRevision, IndexedRevision int64
	Incomplete                      bool
}

// ProjectionWaitRequest binds a wait to one generation and committed revision.
// Callers must provide a context deadline; waits are additionally capped at two minutes.
type ProjectionWaitRequest struct {
	Scopes              []string
	Generation, Purpose string
	Revision            int64
}
type ProjectionReceipt struct {
	Generation      string
	IndexedRevision int64
}

type MemoryRecallItem struct {
	Key    MemoryKey
	Memory MemoryResult
	Score  float64
}
type MemoryRecallResult struct {
	Items                           []MemoryRecallItem
	Generation                      string
	LedgerRevision, IndexedRevision int64
	Incomplete                      bool
}
