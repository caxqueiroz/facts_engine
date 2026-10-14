package model

import (
	"context"
	"fmt"
	"time"
)

// ErrRefreshNotNeeded is distinct from configuration, evidence and schema
// failures so event consumers may skip only an obsolete or restored target.
var ErrRefreshNotNeeded = fmt.Errorf("memory refresh is no longer needed: %w", ErrPrecondition)

type MemoryRefreshTrigger struct {
	EventID        string
	LedgerRevision int64
}
type MemoryRefreshScan struct {
	Scope, Purpose string
	Trigger        MemoryRefreshTrigger
	AfterVersionID string
	Limit          int
}
type MemoryRefreshScanResult struct{ Versions []VersionReference }
type MemoryRefreshScanTx interface {
	Tx
	MemoryRefreshEvent(context.Context, MemoryRefreshTrigger) (Event, error)
	MemoryRefreshDependents(context.Context, string, string, int64, string, int) ([]VersionReference, error)
}
type MemoryRefreshScheduler interface {
	ScanMemoryRefresh(context.Context, Caller, MemoryRefreshScan) (MemoryRefreshScanResult, error)
	StartMemoryRefresh(context.Context, Caller, MemoryRefreshRequest) (JobStatus, error)
}
type MemoryRefreshBinding struct{ Scope, ProfileID string }

func MemoryRefreshConsumer(stream string) string { return "memory-refresh-" + Digest(stream) }
func ChangesMemoryDependency(e Event) bool {
	if e.RecordKind == "memory" || e.RecordID == "" {
		return false
	}
	switch e.Type {
	case "approve_fact", "accept_fact", "supersede_fact", "dispute_fact", "resolve_dispute", "retract_fact", "withdraw_attestation":
		return true
	}
	return false
}

type MemoryRefreshSweepRequest struct {
	Command
	Scope, Stream, ProfileID string
	ScheduledFor             time.Time
	Interval                 time.Duration
}
type MemoryRefreshSweep struct {
	Stream, ProfileID, ProfileDigest string
	ScheduledFor                     time.Time
	Interval                         time.Duration
}
type MemoryRefreshSweepStarter interface {
	StartMemoryRefreshSweep(context.Context, Caller, MemoryRefreshSweepRequest) (CommitResult, error)
}

func IsMemoryRefreshSweep(e Event) bool {
	return e.Type == "sweep_memory_refresh" && e.RefreshSweep != nil
}
