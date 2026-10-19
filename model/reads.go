package model

import (
	"context"
	"time"
)

type ReadOptions struct {
	PaginationKey []byte
	TokenLifetime time.Duration
}
type FactsQuery struct {
	Scope, Subject, Predicate, Purpose string
	ValidAt                            time.Time
	KnowledgeRevision                  int64
	KnownAt                            *time.Time
	Limit                              int
	PageToken                          string
}
type FactsPage struct {
	Items          []SnapshotItem
	LedgerRevision int64
	EvaluatedAt    time.Time
	NextPageToken  string
}

// MemoriesQuery lists one scope's memories as GetMemory would resolve each at
// ValidAt and the selected knowledge revision. Subtype is an optional filter.
type MemoriesQuery struct {
	Scope, Subtype, Purpose string
	ValidAt                 time.Time
	KnowledgeRevision       int64
	KnownAt                 *time.Time
	Limit                   int
	PageToken               string
}
type MemoryListItem struct {
	Key    MemoryKey
	Memory MemoryResult
}
type MemoriesPage struct {
	Items          []MemoryListItem
	LedgerRevision int64
	EvaluatedAt    time.Time
	NextPageToken  string
}
type ReadTx interface {
	TemporalTx
	RevisionAt(context.Context, time.Time) (int64, error)
	FactRecords(context.Context, FactsQuery, int64, string, int) ([]Record, error)
	// MemoryRecords returns memory records in the scope, ordered by record ID,
	// whose key is carried as Record.Key.Scope and Record.Key.Discriminator.
	MemoryRecords(context.Context, MemoriesQuery, int64, string, int) ([]Record, error)
}

// HistoryPageQuery orders one record's immutable events by stream revision.
type HistoryPageQuery struct {
	HistoryQuery
	Limit     int
	PageToken string
}
type HistoryPage struct {
	Events                         []Event
	LedgerRevision, StreamRevision int64
	NextPageToken                  string
}
type HistoryPageTx interface {
	Tx
	HistoryPage(context.Context, string, int64, int64, int) ([]Event, error)
}

type MemoryHistoryPageTx interface {
	Tx
	MemoryHistoryPage(context.Context, string, int64, int64, int) ([]Event, error)
}
