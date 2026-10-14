package workers

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

type Projection struct {
	source     model.ProjectionSource
	index      model.RetrievalIndex
	auth       model.Authorizer
	caller     model.Caller
	generation string
}

func NewProjection(source model.ProjectionSource, index model.RetrievalIndex, auth model.Authorizer, caller model.Caller, generation string) (*Projection, error) {
	if source == nil || index == nil || auth == nil || !model.ValidID(caller.Tenant) || !model.ValidID(caller.Principal) || !model.ValidID(generation) {
		return nil, model.ErrInvalid
	}
	return &Projection{source, index, auth, caller, generation}, nil
}
func (p *Projection) Handle(ctx context.Context, d model.Delivery) error {
	if d.Key.Tenant != p.caller.Tenant || d.Key.Consumer != model.ProjectionConsumer(p.generation) {
		return model.ErrDenied
	}
	switch d.Event.Type {
	case "approve_fact", "accept_fact", "supersede_fact":
	default:
		return nil
	}
	decision, err := p.auth.Check(ctx, model.AuthorizationRequest{Caller: p.caller, Scope: d.Event.Scope, Permission: "can_read", Purpose: "index_projection"})
	if err != nil {
		return model.ErrUnavailable
	}
	if !decision.Allowed {
		return model.ErrDenied
	}
	doc, err := p.source.ProjectionDocument(ctx, d, p.generation)
	if err != nil {
		return err
	}
	if doc == nil {
		return nil
	}
	if time.Since(decision.CheckedAt) > 30*time.Second {
		return model.ErrUnavailable
	}
	return p.index.Put(ctx, *doc)
}
