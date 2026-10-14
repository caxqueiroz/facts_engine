package model

import "context"
import "time"

// ProjectionDocument contains only canonical accepted fact content, never quotes.
type ProjectionDocument struct {
	Tenant, Generation, RecordID, VersionID, VersionDigest, PolicyID, Classification string
	Key                                                                              FactKey
	Value                                                                            Value
	Valid                                                                            Interval
	SourceClasses                                                                    []string
}

func ProjectionID(d ProjectionDocument) string {
	return Digest([]string{"facts-projection-v1", d.Tenant, d.Generation, d.VersionID})
}
func ProjectionConsumer(generation string) string { return "projection-" + Digest(generation) }

type ProjectionSource interface {
	ProjectionDocument(context.Context, Delivery, string) (*ProjectionDocument, error)
}
type SearchCandidate struct {
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
