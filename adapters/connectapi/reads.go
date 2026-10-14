package connectapi

import (
	"context"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

// Engine is the shared application backend required by the registered services.
// The public facts_engine.Engine satisfies it.
type Engine interface {
	FactEngine
	SnapshotEngine
	EvidenceEngine
	DefinitionEngine
	AttestationEngine
	MemoryEngine
	IngestionEngine
	ExtractionEngine
	CalculationEngine
	AuditEngine
	RetrievalEngine
}

type SnapshotEngine interface {
	CreateFactSnapshot(context.Context, model.Caller, model.FactSnapshotRequest) (model.FactSnapshot, error)
	GetFactSnapshot(context.Context, model.Caller, model.SnapshotQuery) (model.FactSnapshot, error)
}
type snapshotService struct{ engine SnapshotEngine }

func (s *factService) ListFacts(ctx context.Context, r *v1.ListFactsRequest) (*v1.ListFactsResponse, error) {
	q := model.FactsQuery{Scope: r.Scope, Subject: r.Subject, Predicate: r.Predicate, Purpose: r.Purpose, ValidAt: r.ValidAt.AsTime(), KnowledgeRevision: r.KnowledgeRevision, KnownAt: optionalTime(r.KnownAt), Limit: int(r.Limit), PageToken: r.PageToken}
	out, err := s.engine.ListFacts(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.ListFactsResponse{LedgerRevision: out.LedgerRevision, EvaluatedAt: c.timestamp(out.EvaluatedAt), NextPageToken: out.NextPageToken}
	for _, item := range out.Items {
		response.Items = append(response.Items, c.snapshotItem(item))
	}
	return response, c.err
}
func (s *factService) GetFactHistory(ctx context.Context, r *v1.GetFactHistoryRequest) (*v1.GetFactHistoryResponse, error) {
	q := model.HistoryPageQuery{HistoryQuery: model.HistoryQuery{Scope: r.Scope, RecordID: r.RecordId, Purpose: r.Purpose}, Limit: int(r.Limit), PageToken: r.PageToken}
	out, err := s.engine.ListFactHistory(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetFactHistoryResponse{LedgerRevision: out.LedgerRevision, StreamRevision: out.StreamRevision, NextPageToken: out.NextPageToken}
	for _, event := range out.Events {
		response.Events = append(response.Events, c.event(event))
	}
	return response, c.err
}
func (s *snapshotService) CreateFactSnapshot(ctx context.Context, r *v1.CreateFactSnapshotRequest) (*v1.CreateFactSnapshotResponse, error) {
	q := model.FactSnapshotRequest{Command: command(r.Command), ValidAt: r.ValidAt.AsTime(), KnowledgeRevision: r.KnowledgeRevision}
	for _, key := range r.Selectors {
		q.Selectors = append(q.Selectors, factKey(key))
	}
	out, err := s.engine.CreateFactSnapshot(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.CreateFactSnapshotResponse{Snapshot: c.snapshot(out)}
	return response, c.err
}
func (s *snapshotService) GetFactSnapshot(ctx context.Context, r *v1.GetFactSnapshotRequest) (*v1.GetFactSnapshotResponse, error) {
	out, err := s.engine.GetFactSnapshot(ctx, callerFrom(ctx), model.SnapshotQuery{ID: r.Id, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetFactSnapshotResponse{Snapshot: c.snapshot(out)}
	return response, c.err
}
