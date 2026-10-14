package connectapi

import (
	"context"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

// FactEngine is satisfied by the public Engine; handlers contain no acceptance policy.
type FactEngine interface {
	ListFacts(context.Context, model.Caller, model.FactsQuery) (model.FactsPage, error)
	ListFactHistory(context.Context, model.Caller, model.HistoryPageQuery) (model.HistoryPage, error)
	GetFact(context.Context, model.Caller, model.FactQuery) (model.FactResult, error)
	ProposeFact(context.Context, model.Caller, model.FactProposal) (model.CommitResult, error)
	ApproveFact(context.Context, model.Caller, model.Approval) (model.CommitResult, error)
	AcceptFact(context.Context, model.Caller, model.Approval) (model.CommitResult, error)
	RejectFact(context.Context, model.Caller, model.LifecycleChange) (model.CommitResult, error)
	RetractFact(context.Context, model.Caller, model.LifecycleChange) (model.CommitResult, error)
	DisputeFact(context.Context, model.Caller, model.LifecycleChange) (model.CommitResult, error)
	ResolveDispute(context.Context, model.Caller, model.LifecycleChange) (model.CommitResult, error)
	SupersedeFact(context.Context, model.Caller, model.Supersession) (model.CommitResult, error)
}
type factService struct{ engine FactEngine }

func (s *factService) GetFact(ctx context.Context, r *v1.GetFactRequest) (*v1.GetFactResponse, error) {
	q := model.FactQuery{Purpose: r.Purpose, Key: factKey(r.Key), ValidAt: r.ValidAt.AsTime(), KnowledgeRevision: r.KnowledgeRevision, KnownAt: optionalTime(r.KnownAt)}
	out, err := s.engine.GetFact(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	converter := responseConverter{}
	result := converter.factResult(out)
	return &v1.GetFactResponse{Result: result}, converter.err
}
func (s *factService) ProposeFact(ctx context.Context, r *v1.ProposeFactRequest) (*v1.ProposeFactResponse, error) {
	value, err := valueFromProto(r.Value)
	if err != nil {
		return nil, err
	}
	q := model.FactProposal{Command: command(r.Command), Key: factKey(r.Key), SchemaVersion: r.SchemaVersion, Value: value, Valid: interval(r.Valid), ExpectedRevision: r.ExpectedRevision}
	q.Sources, err = sourcesFromProto(r.Sources)
	if err != nil {
		return nil, err
	}
	out, err := s.engine.ProposeFact(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.ProposeFactResponse{Result: commit(out)}, nil
}
func (s *factService) ApproveFact(ctx context.Context, r *v1.ApproveFactRequest) (*v1.ApproveFactResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := a
	out, err := s.engine.ApproveFact(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.ApproveFactResponse{Result: commit(out)}, nil
}
func (s *factService) AcceptFact(ctx context.Context, r *v1.AcceptFactRequest) (*v1.AcceptFactResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := a
	out, err := s.engine.AcceptFact(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.AcceptFactResponse{Result: commit(out)}, nil
}
func (s *factService) RejectFact(ctx context.Context, r *v1.RejectFactRequest) (*v1.RejectFactResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := model.LifecycleChange{Approval: a, Reason: r.Reason}
	out, err := s.engine.RejectFact(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.RejectFactResponse{Result: commit(out)}, nil
}
func (s *factService) RetractFact(ctx context.Context, r *v1.RetractFactRequest) (*v1.RetractFactResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := model.LifecycleChange{Approval: a, Reason: r.Reason, Valid: interval(r.Valid)}
	out, err := s.engine.RetractFact(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.RetractFactResponse{Result: commit(out)}, nil
}
func (s *factService) DisputeFact(ctx context.Context, r *v1.DisputeFactRequest) (*v1.DisputeFactResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := model.LifecycleChange{Approval: a, Reason: r.Reason, Valid: interval(r.Valid)}
	out, err := s.engine.DisputeFact(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.DisputeFactResponse{Result: commit(out)}, nil
}
func (s *factService) ResolveDispute(ctx context.Context, r *v1.ResolveDisputeRequest) (*v1.ResolveDisputeResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := model.LifecycleChange{Approval: a, Reason: r.Reason, Valid: interval(r.Valid)}
	out, err := s.engine.ResolveDispute(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.ResolveDisputeResponse{Result: commit(out)}, nil
}
func (s *factService) SupersedeFact(ctx context.Context, r *v1.SupersedeFactRequest) (*v1.SupersedeFactResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := model.Supersession{Approval: a, Reason: r.Reason, Valid: interval(r.Valid), ReplacedVersionIDs: r.ReplacedVersionIds, Automated: r.Automated}
	out, err := s.engine.SupersedeFact(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.SupersedeFactResponse{Result: commit(out)}, nil
}
