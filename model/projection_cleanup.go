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
