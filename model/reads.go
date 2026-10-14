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
type ReadTx interface {
	TemporalTx
	RevisionAt(context.Context, time.Time) (int64, error)
	FactRecords(context.Context, FactsQuery, int64, string, int) ([]Record, error)
}
