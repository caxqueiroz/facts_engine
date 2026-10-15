package model

import "context"

// ProjectionDisposal identifies one immutable version in one destination.
// Kind is explicitly "fact" or "memory". This adapter contract is not disposal
// authorization: the host must coordinate ledger policy, holds and every copy.
type ProjectionDisposal struct{ Tenant, Generation, Kind, VersionID string }

// ProjectionDisposer atomically removes content and leaves a durable fence
// against delayed writes. Implementations must not delete a sealed generation.
type ProjectionDisposer interface {
	DisposeProjection(context.Context, ProjectionDisposal) error
}
