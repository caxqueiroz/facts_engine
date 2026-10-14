package workers

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
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
	case "approve_fact", "accept_fact", "supersede_fact", "approve_memory", "accept_memory", "supersede_memory":
	default:
		return nil
	}
	decision, err := p.auth.Check(ctx, model.AuthorizationRequest{Caller: p.caller, Scope: d.Event.Scope, Permission: "can_read", Purpose: "index_projection"})
	if err != nil {
		return model.ErrUnavailable
	}
	if err := validateWorkerAuthorization(decision); err != nil {
		return err
	}
	doc, err := p.source.ProjectionDocument(ctx, d, p.generation)
	if err != nil {
		return err
	}
	if doc == nil {
		return nil
	}
	checks := []model.AuthorizationDecision{decision}
	if doc.Memory != nil {
		scopes := map[string]bool{doc.Memory.Key.Scope: true}
		for _, dependency := range doc.Memory.Dependencies {
			scopes[dependency.Scope] = true
		}
		for scope := range scopes {
			for _, permission := range []string{"can_read", "can_read_history", "can_read_evidence"} {
				check, err := p.auth.Check(ctx, model.AuthorizationRequest{Caller: p.caller, Scope: scope, Permission: permission, Purpose: "index_projection"})
				if err != nil {
					return model.ErrUnavailable
				}
				if err := validateWorkerAuthorization(check); err != nil {
					return err
				}
				checks = append(checks, check)
			}
		}
	}
	for _, check := range checks {
		if err := validateWorkerAuthorization(check); err != nil {
			return err
		}
	}
	return p.index.Put(ctx, *doc)
}
