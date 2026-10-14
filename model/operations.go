package model

// OperationsOptions uses separately provisioned administrative ports. Scope is
// host-owned and grants tenant-wide projection operations through can_operate.
type OperationsOptions struct {
	Scope    string
	Store    RebuildStore
	Index    RebuildIndex
	Progress Outbox
}
type ProjectionStatusQuery struct{ Generation, Purpose string }
type ProjectionStatus struct {
	Active         ActiveProjection
	Rebuild        RebuildStatus
	Consumer       ConsumerStatus
	LedgerRevision int64
	Incomplete     bool
}

// RebuildRequest selects an explicit administrative transition. Generation is
// also the start identity; retries cannot change its configured attempt budget.
type RebuildRequest struct {
	Generation, Action, Purpose string
	MaxAttempts                 int
	ExpectedFence               int64
}
