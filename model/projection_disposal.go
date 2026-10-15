package model

import "context"

// ProjectionDisposal identifies one immutable version in one destination.
// Kind is explicitly "fact" or "memory". This adapter contract is not disposal
// authorization: the host must coordinate ledger policy, holds and every copy.
type ProjectionDisposal struct{ Tenant, Generation, Kind, VersionID string }

// ProjectionDisposer atomically removes content and leaves a durable fence
// against delayed writes. A sealed generation must first enter the permanent
// disposal state through ProjectionDisposalSealer before accepting new cleanup.
type ProjectionDisposer interface {
	DisposeProjection(context.Context, ProjectionDisposal) error
}

// ProjectionDisposalSealer permanently excludes a generation from verification
// and activation, while allowing individual projection cleanup. The host must
// authorize disposal and establish a retired/abandoned ledger state first.
// fence is the exact sealed generation fence. An exact retry is idempotent;
// the transition must reject stale fences and prevent any future reopening.
type ProjectionDisposalSealer interface {
	SealProjectionForDisposal(context.Context, GenerationKey, int64) error
}
