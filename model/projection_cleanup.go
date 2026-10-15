package model

import (
	"context"
	"time"
)

// ProjectionCleanupKey identifies index cleanup for one disposed version.
type ProjectionCleanupKey struct{ Tenant, Kind, VersionID string }

type ProjectionCleanupTarget struct {
	Generation, AcknowledgementID string
	CompletedAt                   *time.Time
}

// ProjectionCleanupStatus reports acknowledgements for the destinations captured
// atomically with a version tombstone. Complete means every captured target has
// an acknowledgement; it does not prove independent destination state, authorize
// disposal, or claim that evidence, derivatives, backups or archives were erased.
type ProjectionCleanupStatus struct {
	Key            ProjectionCleanupKey
	InventoryEpoch int64
	CreatedAt      time.Time
	Targets        []ProjectionCleanupTarget
	Complete       bool
}

// ProjectionCleanupReader is a trusted maintenance port, not an end-user API.
type ProjectionCleanupReader interface {
	ProjectionCleanupStatus(context.Context, ProjectionCleanupKey) (ProjectionCleanupStatus, error)
}

// ProjectionCleanupRequest selects one captured destination and binds it to the
// original policy-authorized version disposal. Bindings must match that event.
type ProjectionCleanupRequest struct {
	Command
	Kind, Scope, RecordID, VersionID, DisposalEventID, Generation, Reason string
	Bindings                                                              []VersionRetentionBinding
}

// ProjectionCleanupPlan is prepared while the transaction holds scope and
// generation guards. A positive TerminalFence requires permanent destination
// sealing before cleanup; only retired/abandoned generations may receive it.
type ProjectionCleanupPlan struct {
	Target        ProjectionDisposal
	TerminalFence int64
}

type ProjectionCleanupTx interface {
	PrepareProjectionCleanup(context.Context, ProjectionCleanupRequest) (ProjectionCleanupPlan, error)
}

// ProjectionCleanupOptions uses separately provisioned retention credentials.
// Destinations must implement idempotent atomic deletion with durable write
// fences, and honor context cancellation. Terminal generations also require
// ProjectionCleanupGate. No connections or workers are started by the engine.
type ProjectionCleanupOptions struct{ Destination ProjectionDisposer }

type ProjectionCleanupGate interface {
	ProjectionDisposalSealer
	SetProjectionGate(context.Context, GenerationKey, int64, bool) error
}
