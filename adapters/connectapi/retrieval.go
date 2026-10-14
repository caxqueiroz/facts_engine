package connectapi

import (
	"context"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type RetrievalEngine interface {
	Recall(context.Context, model.Caller, model.RecallRequest) (model.RecallResult, error)
	RecallMemories(context.Context, model.Caller, model.RecallRequest) (model.MemoryRecallResult, error)
	WaitForProjection(context.Context, model.Caller, model.ProjectionWaitRequest) (model.ProjectionReceipt, error)
}
type retrievalService struct{ engine RetrievalEngine }

func recallQuery(q *v1.RecallQuery) model.RecallRequest {
	return model.RecallRequest{Scopes: q.Scopes, Query: q.Query, Purpose: q.Purpose, ValidAt: q.ValidAt.AsTime(), KnowledgeRevision: q.KnowledgeRevision, RequireRevision: q.RequireRevision, RequireGeneration: q.RequireGeneration, Limit: int(q.Limit)}
}
func (s *retrievalService) Recall(ctx context.Context, r *v1.RecallRequest) (*v1.RecallResponse, error) {
	out, err := s.engine.Recall(ctx, callerFrom(ctx), recallQuery(r.Query))
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.RecallResponse{Generation: out.Generation, LedgerRevision: out.LedgerRevision, IndexedRevision: out.IndexedRevision, Incomplete: out.Incomplete}
	for _, item := range out.Items {
		response.Items = append(response.Items, &v1.RecallItem{Key: keyToProto(item.Key), Fact: c.factResult(item.Fact), Score: item.Score})
	}
	return response, c.err
}
func memoryResult(r model.MemoryResult) *v1.MemoryResult {
	return &v1.MemoryResult{State: r.State, RecordId: r.RecordID, VersionId: r.VersionID, Body: r.Body, StreamRevision: r.StreamRevision, LedgerRevision: r.LedgerRevision, Dependencies: dependenciesToProto(r.Dependencies)}
}
func (s *retrievalService) RecallMemories(ctx context.Context, r *v1.RecallMemoriesRequest) (*v1.RecallMemoriesResponse, error) {
	out, err := s.engine.RecallMemories(ctx, callerFrom(ctx), recallQuery(r.Query))
	if err != nil {
		return nil, err
	}
	response := &v1.RecallMemoriesResponse{Generation: out.Generation, LedgerRevision: out.LedgerRevision, IndexedRevision: out.IndexedRevision, Incomplete: out.Incomplete}
	for _, item := range out.Items {
		response.Items = append(response.Items, &v1.MemoryRecallItem{Key: &v1.MemoryKey{Scope: item.Key.Scope, Id: item.Key.ID}, Memory: memoryResult(item.Memory), Score: item.Score})
	}
	return response, nil
}
func (s *retrievalService) WaitForProjection(ctx context.Context, r *v1.WaitForProjectionRequest) (*v1.WaitForProjectionResponse, error) {
	out, err := s.engine.WaitForProjection(ctx, callerFrom(ctx), model.ProjectionWaitRequest{Scopes: r.Scopes, Generation: r.Generation, Purpose: r.Purpose, Revision: r.Revision})
	if err != nil {
		return nil, err
	}
	return &v1.WaitForProjectionResponse{Generation: out.Generation, IndexedRevision: out.IndexedRevision}, nil
}
