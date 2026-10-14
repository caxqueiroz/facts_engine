package connectapi

import (
	"context"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type IdentityEngine interface {
	PutScope(context.Context, model.Caller, model.ScopeRequest) (model.CommitResult, error)
	PutSubject(context.Context, model.Caller, model.SubjectRequest) (model.CommitResult, error)
	GetScope(context.Context, model.Caller, model.IdentityQuery) (model.ScopeVersion, error)
	GetSubject(context.Context, model.Caller, model.IdentityQuery) (model.SubjectVersion, error)
	LookupSubject(context.Context, model.Caller, model.SubjectLookupQuery) (model.SubjectVersion, error)
}
type identityService struct{ engine IdentityEngine }

func (s *identityService) PutScope(ctx context.Context, r *v1.PutScopeRequest) (*v1.PutScopeResponse, error) {
	out, err := s.engine.PutScope(ctx, callerFrom(ctx), model.ScopeRequest{Command: command(r.Command), Definition: model.ScopeDefinition{ID: r.Definition.Id, Label: r.Definition.Label}, ExpectedRevision: r.ExpectedRevision, Reason: r.Reason})
	if err != nil {
		return nil, err
	}
	return &v1.PutScopeResponse{Result: commit(out)}, nil
}
func (s *identityService) PutSubject(ctx context.Context, r *v1.PutSubjectRequest) (*v1.PutSubjectResponse, error) {
	d := r.Definition
	out, err := s.engine.PutSubject(ctx, callerFrom(ctx), model.SubjectRequest{Command: command(r.Command), Definition: model.SubjectDefinition{Scope: d.Scope, ID: d.Id, Kind: d.Kind, Label: d.Label, Lookup: d.Lookup}, ExpectedRevision: r.ExpectedRevision, Reason: r.Reason})
	if err != nil {
		return nil, err
	}
	return &v1.PutSubjectResponse{Result: commit(out)}, nil
}
func (s *identityService) GetScope(ctx context.Context, r *v1.GetScopeRequest) (*v1.GetScopeResponse, error) {
	out, err := s.engine.GetScope(ctx, callerFrom(ctx), model.IdentityQuery{Scope: r.Scope, Purpose: r.Purpose, Revision: r.Revision})
	if err != nil {
		return nil, err
	}
	return &v1.GetScopeResponse{Version: &v1.ScopeVersion{Definition: &v1.ScopeDefinition{Id: out.Definition.ID, Label: out.Definition.Label}, Revision: out.Revision, LedgerRevision: out.LedgerRevision, Caller: callerToProto(out.Caller), Reason: out.Reason}}, nil
}
func subjectVersion(v model.SubjectVersion) *v1.SubjectVersion {
	d := v.Definition
	return &v1.SubjectVersion{Definition: &v1.SubjectDefinition{Scope: d.Scope, Id: d.ID, Kind: d.Kind, Label: d.Label, Lookup: d.Lookup}, Revision: v.Revision, LedgerRevision: v.LedgerRevision, Caller: callerToProto(v.Caller), Reason: v.Reason}
}
func (s *identityService) GetSubject(ctx context.Context, r *v1.GetSubjectRequest) (*v1.GetSubjectResponse, error) {
	out, err := s.engine.GetSubject(ctx, callerFrom(ctx), model.IdentityQuery{Scope: r.Scope, ID: r.Id, Purpose: r.Purpose, Revision: r.Revision})
	if err != nil {
		return nil, err
	}
	return &v1.GetSubjectResponse{Version: subjectVersion(out)}, nil
}
func (s *identityService) LookupSubject(ctx context.Context, r *v1.LookupSubjectRequest) (*v1.LookupSubjectResponse, error) {
	out, err := s.engine.LookupSubject(ctx, callerFrom(ctx), model.SubjectLookupQuery{Scope: r.Scope, Namespace: r.Namespace, Value: r.Value, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	return &v1.LookupSubjectResponse{Version: subjectVersion(out)}, nil
}
