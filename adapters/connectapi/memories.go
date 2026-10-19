package connectapi

import (
	"context"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type MemoryEngine interface {
	GetMemory(context.Context, model.Caller, model.MemoryQuery) (model.MemoryResult, error)
	ProposeMemory(context.Context, model.Caller, model.MemoryProposal) (model.CommitResult, error)
	ApproveMemory(context.Context, model.Caller, model.Approval) (model.CommitResult, error)
	AcceptMemory(context.Context, model.Caller, model.Approval) (model.CommitResult, error)
	RejectMemory(context.Context, model.Caller, model.LifecycleChange) (model.CommitResult, error)
	RetractMemory(context.Context, model.Caller, model.LifecycleChange) (model.CommitResult, error)
	DisputeMemory(context.Context, model.Caller, model.LifecycleChange) (model.CommitResult, error)
	ResolveMemoryDispute(context.Context, model.Caller, model.LifecycleChange) (model.CommitResult, error)
	SupersedeMemory(context.Context, model.Caller, model.Supersession) (model.CommitResult, error)
	GetMemoryVersion(context.Context, model.Caller, model.MemoryVersionQuery) (model.MemoryVersion, error)
	GetMemoryHistory(context.Context, model.Caller, model.HistoryPageQuery) (model.HistoryPage, error)
	ListMemories(context.Context, model.Caller, model.MemoriesQuery) (model.MemoriesPage, error)
}
type memoryService struct{ engine MemoryEngine }

func (s *memoryService) ApproveMemory(ctx context.Context, r *v1.ApproveMemoryRequest) (*v1.ApproveMemoryResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := a
	out, err := s.engine.ApproveMemory(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.ApproveMemoryResponse{Result: commit(out)}, nil
}
func (s *memoryService) AcceptMemory(ctx context.Context, r *v1.AcceptMemoryRequest) (*v1.AcceptMemoryResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := a
	out, err := s.engine.AcceptMemory(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.AcceptMemoryResponse{Result: commit(out)}, nil
}
func (s *memoryService) RejectMemory(ctx context.Context, r *v1.RejectMemoryRequest) (*v1.RejectMemoryResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := model.LifecycleChange{Approval: a, Reason: r.Reason}
	out, err := s.engine.RejectMemory(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.RejectMemoryResponse{Result: commit(out)}, nil
}
func (s *memoryService) RetractMemory(ctx context.Context, r *v1.RetractMemoryRequest) (*v1.RetractMemoryResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := model.LifecycleChange{Approval: a, Reason: r.Reason, Valid: interval(r.Valid)}
	out, err := s.engine.RetractMemory(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.RetractMemoryResponse{Result: commit(out)}, nil
}
func (s *memoryService) DisputeMemory(ctx context.Context, r *v1.DisputeMemoryRequest) (*v1.DisputeMemoryResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := model.LifecycleChange{Approval: a, Reason: r.Reason, Valid: interval(r.Valid)}
	out, err := s.engine.DisputeMemory(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.DisputeMemoryResponse{Result: commit(out)}, nil
}
func (s *memoryService) ResolveMemoryDispute(ctx context.Context, r *v1.ResolveMemoryDisputeRequest) (*v1.ResolveMemoryDisputeResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := model.LifecycleChange{Approval: a, Reason: r.Reason, Valid: interval(r.Valid)}
	out, err := s.engine.ResolveMemoryDispute(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.ResolveMemoryDisputeResponse{Result: commit(out)}, nil
}
func (s *memoryService) SupersedeMemory(ctx context.Context, r *v1.SupersedeMemoryRequest) (*v1.SupersedeMemoryResponse, error) {
	a := model.Approval{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ExpectedRevision: r.ExpectedRevision}
	q := model.Supersession{Approval: a, Reason: r.Reason, Valid: interval(r.Valid), ReplacedVersionIDs: r.ReplacedVersionIds, Automated: r.Automated}
	out, err := s.engine.SupersedeMemory(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.SupersedeMemoryResponse{Result: commit(out)}, nil
}
func (s *memoryService) ProposeMemory(ctx context.Context, r *v1.ProposeMemoryRequest) (*v1.ProposeMemoryResponse, error) {
	p := r.Proposal
	sources, err := sourcesFromProto(p.Sources)
	if err != nil {
		return nil, err
	}
	q := model.MemoryProposal{Command: command(p.Command), Key: model.MemoryKey{Scope: p.Key.Scope, ID: p.Key.Id}, Subtype: p.Subtype, SchemaVersion: p.SchemaVersion, Body: p.Body, Valid: interval(p.Valid), Sources: sources, ExpectedRevision: p.ExpectedRevision}
	for _, d := range p.Dependencies {
		q.Dependencies = append(q.Dependencies, model.MemoryDependency{Scope: d.Scope, RecordID: d.RecordId, VersionID: d.VersionId, ContentDigest: d.ContentDigest, Label: d.Label})
	}
	out, err := s.engine.ProposeMemory(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.ProposeMemoryResponse{Result: commit(out)}, nil
}
func (s *memoryService) GetMemory(ctx context.Context, r *v1.GetMemoryRequest) (*v1.GetMemoryResponse, error) {
	q := model.MemoryQuery{Key: model.MemoryKey{Scope: r.Key.Scope, ID: r.Key.Id}, Purpose: r.Purpose, ValidAt: r.ValidAt.AsTime(), KnowledgeRevision: r.KnowledgeRevision, KnownAt: optionalTime(r.KnownAt)}
	out, err := s.engine.GetMemory(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.GetMemoryResponse{Result: memoryResult(out)}, nil
}
func (s *memoryService) GetMemoryVersion(ctx context.Context, r *v1.GetMemoryVersionRequest) (*v1.GetMemoryVersionResponse, error) {
	out, err := s.engine.GetMemoryVersion(ctx, callerFrom(ctx), model.MemoryVersionQuery{Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetMemoryVersionResponse{Version: c.memoryVersion(out)}
	return response, c.err
}
func (s *memoryService) GetMemoryHistory(ctx context.Context, r *v1.GetMemoryHistoryRequest) (*v1.GetMemoryHistoryResponse, error) {
	out, err := s.engine.GetMemoryHistory(ctx, callerFrom(ctx), model.HistoryPageQuery{HistoryQuery: model.HistoryQuery{Scope: r.Scope, RecordID: r.RecordId, Purpose: r.Purpose}, Limit: int(r.Limit), PageToken: r.PageToken})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetMemoryHistoryResponse{LedgerRevision: out.LedgerRevision, StreamRevision: out.StreamRevision, NextPageToken: out.NextPageToken}
	for _, event := range out.Events {
		response.Events = append(response.Events, c.event(event))
	}
	return response, c.err
}
func (s *memoryService) ListMemories(ctx context.Context, r *v1.ListMemoriesRequest) (*v1.ListMemoriesResponse, error) {
	q := model.MemoriesQuery{Scope: r.Scope, Subtype: r.Subtype, Purpose: r.Purpose, ValidAt: r.ValidAt.AsTime(), KnowledgeRevision: r.KnowledgeRevision, KnownAt: optionalTime(r.KnownAt), Limit: int(r.Limit), PageToken: r.PageToken}
	out, err := s.engine.ListMemories(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.ListMemoriesResponse{LedgerRevision: out.LedgerRevision, EvaluatedAt: c.timestamp(out.EvaluatedAt), NextPageToken: out.NextPageToken}
	for _, item := range out.Items {
		response.Items = append(response.Items, &v1.MemoryListItem{Key: &v1.MemoryKey{Scope: item.Key.Scope, Id: item.Key.ID}, Memory: memoryResult(item.Memory)})
	}
	return response, c.err
}
func dependenciesToProto(dependencies []model.MemoryDependency) []*v1.MemoryDependency {
	var out []*v1.MemoryDependency
	for _, d := range dependencies {
		out = append(out, &v1.MemoryDependency{Scope: d.Scope, RecordId: d.RecordID, VersionId: d.VersionID, ContentDigest: d.ContentDigest, Label: d.Label})
	}
	return out
}
func (c *responseConverter) memoryVersion(v model.MemoryVersion) *v1.MemoryVersion {
	p := v.Proposal
	proposal := &v1.RetainedMemoryProposal{Purpose: p.Purpose, CorrelationId: p.CorrelationID, Key: &v1.MemoryKey{Scope: p.Key.Scope, Id: p.Key.ID}, Subtype: p.Subtype, SchemaVersion: p.SchemaVersion, Body: p.Body, Valid: c.interval(p.Valid), Dependencies: dependenciesToProto(p.Dependencies), ExpectedRevision: p.ExpectedRevision}
	for _, span := range p.Sources {
		proposal.Sources = append(proposal.Sources, &v1.SourceSpan{EvidenceId: span.EvidenceID, Start: int64(span.Start), End: int64(span.End), Quote: span.Quote})
	}
	out := &v1.MemoryVersion{Id: v.ID, RecordId: v.RecordID, Digest: v.Digest, PolicyId: v.PolicyID, Proposal: proposal, Proposer: callerToProto(v.Proposer), LedgerRevision: v.LedgerRevision}
	for _, a := range v.Authorization {
		out.Authorization = append(out.Authorization, c.authorization(a))
	}
	return out
}
