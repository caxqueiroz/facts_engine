package connectapi

import (
	"context"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type DefinitionEngine interface {
	RegisterMemoryType(context.Context, model.Caller, model.MemoryTypeRequest) (model.CommitResult, error)
	RegisterAcceptancePolicy(context.Context, model.Caller, model.PolicyRequest) (model.CommitResult, error)
	RegisterPredicateVersion(context.Context, model.Caller, model.PredicateRequest) (model.CommitResult, error)
}
type definitionService struct{ engine DefinitionEngine }

func (s *definitionService) RegisterAcceptancePolicy(ctx context.Context, r *v1.RegisterAcceptancePolicyRequest) (*v1.RegisterAcceptancePolicyResponse, error) {
	p := r.Policy
	policy := model.AcceptancePolicy{Scope: p.Scope, ID: p.Id, Mode: p.Mode, AcceptingServices: p.AcceptingServices, SourceClasses: p.SourceClasses}
	for _, required := range p.Required {
		policy.Required = append(policy.Required, model.AttestationRequirement{Kind: required.Kind, Principals: required.Principals, RequireExpiry: required.RequireExpiry, MaxAgeSeconds: required.MaxAgeSeconds})
	}
	out, err := s.engine.RegisterAcceptancePolicy(ctx, callerFrom(ctx), model.PolicyRequest{Command: command(r.Command), Policy: policy})
	if err != nil {
		return nil, err
	}
	return &v1.RegisterAcceptancePolicyResponse{Result: commit(out)}, nil
}
func (s *definitionService) RegisterPredicateVersion(ctx context.Context, r *v1.RegisterPredicateVersionRequest) (*v1.RegisterPredicateVersionResponse, error) {
	d := r.Definition
	definition := model.PredicateVersion{Scope: d.Scope, Predicate: d.Predicate, Version: d.Version, PolicyID: d.PolicyId, Kind: model.ValueKind(d.Kind), Classification: d.Classification, Cardinality: d.Cardinality, TemporalPrecision: d.TemporalPrecision}
	if d.Migration != nil {
		definition.Migration = &model.PredicateMigration{FromVersion: d.Migration.FromVersion, Mode: d.Migration.Mode, Reason: d.Migration.Reason}
	}
	if d.Constraints != nil {
		c := d.Constraints
		definition.Constraints = &model.ValueConstraints{AllowUnknown: c.AllowUnknown, MinTextRunes: optionalInt(c.MinTextRunes), MaxTextRunes: optionalInt(c.MaxTextRunes), MinInteger: c.MinInteger, MaxInteger: c.MaxInteger, MinDecimal: c.MinDecimal, MaxDecimal: c.MaxDecimal, DecimalPlaces: optionalInt(c.DecimalPlaces), Units: c.Units}
		for _, value := range c.AllowedValues {
			v, err := valueFromProto(value)
			if err != nil {
				return nil, err
			}
			definition.Constraints.AllowedValues = append(definition.Constraints.AllowedValues, v)
		}
	}
	out, err := s.engine.RegisterPredicateVersion(ctx, callerFrom(ctx), model.PredicateRequest{Command: command(r.Command), Definition: definition})
	if err != nil {
		return nil, err
	}
	return &v1.RegisterPredicateVersionResponse{Result: commit(out)}, nil
}
func optionalInt(value *int32) *int {
	if value == nil {
		return nil
	}
	result := int(*value)
	return &result
}

func (s *definitionService) RegisterMemoryType(ctx context.Context, r *v1.RegisterMemoryTypeRequest) (*v1.RegisterMemoryTypeResponse, error) {
	d := r.Definition
	definition := model.MemoryType{Scope: d.Scope, Subtype: d.Subtype, Version: d.Version, PolicyID: d.PolicyId, Classification: d.Classification}
	if d.Migration != nil {
		definition.Migration = &model.PredicateMigration{FromVersion: d.Migration.FromVersion, Mode: d.Migration.Mode, Reason: d.Migration.Reason}
	}
	out, err := s.engine.RegisterMemoryType(ctx, callerFrom(ctx), model.MemoryTypeRequest{Command: command(r.Command), Definition: definition})
	if err != nil {
		return nil, err
	}
	return &v1.RegisterMemoryTypeResponse{Result: commit(out)}, nil
}
