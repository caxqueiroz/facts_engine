package model

import "context"

type GenerationKey struct{ Tenant, Generation string }
type ActiveProjection struct {
	Generation string
	Epoch      int64
}
type RebuildStatus struct {
	VerificationVersion               string `json:",omitempty"`
	Key                               GenerationKey
	State                             string
	Fence, ThroughRevision, BaseEpoch int64
	PreviousGeneration                string
	Count                             int
	Digest                            string
}
type ProjectionCheck struct {
	Version string `json:",omitempty"`
	Count   int
	Digest  string
}

// RebuildActor is a trusted operator identity supplied by the host. The
// administrative port must not be exposed as an unchecked end-user API.
type RebuildActor struct {
	Principal, Purpose string
	// Present for application-authorized operations; legacy trusted worker callers
	// retain their host-supplied principal and purpose contract.
	Caller        *Caller                `json:",omitempty"`
	Authorization *AuthorizationDecision `json:",omitempty"`
}
type ProjectionRegistry interface {
	ActiveProjection(context.Context, string) (ActiveProjection, error)
}
type RebuildStore interface {
	ProjectionRegistry
	StartRebuild(context.Context, GenerationKey, RebuildActor, int) (RebuildStatus, error)
	RebuildStatus(context.Context, GenerationKey) (RebuildStatus, error)
	FreezeRebuild(context.Context, GenerationKey, RebuildActor) (RebuildStatus, error)
	VerifyRebuild(context.Context, GenerationKey, RebuildActor, int64, ProjectionCheck) (RebuildStatus, error)
	ResumeRebuild(context.Context, GenerationKey, RebuildActor, int64) (RebuildStatus, error)
	FinishResume(context.Context, GenerationKey, RebuildActor, int64) (RebuildStatus, error)
	ActivateRebuild(context.Context, GenerationKey, RebuildActor, int64) (RebuildStatus, error)
	FinishActivation(context.Context, GenerationKey, RebuildActor, int64) (RebuildStatus, error)
	AbandonRebuild(context.Context, GenerationKey, RebuildActor, int64) (RebuildStatus, error)
	ProjectionDocuments(context.Context, GenerationKey, int64) ([]ProjectionDocument, error)
}
type RebuildIndex interface {
	RetrievalIndex
	SetProjectionGate(context.Context, GenerationKey, int64, bool) error
	VerifyProjection(context.Context, GenerationKey, int64, []ProjectionDocument) (ProjectionCheck, error)
}
