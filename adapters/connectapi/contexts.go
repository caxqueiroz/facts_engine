package connectapi

import (
	"context"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type ContextEngine interface {
	PrepareContext(context.Context, model.Caller, model.ContextRequest) (model.ContextManifest, error)
	GetContextManifest(context.Context, model.Caller, model.ContextQuery) (model.ContextManifest, error)
	RecordDecision(context.Context, model.Caller, model.DecisionReceipt) (model.DecisionRecord, error)
	GetDecisionReceipt(context.Context, model.Caller, model.DecisionQuery) (model.DecisionRecord, error)
}
type contextService struct{ engine ContextEngine }

func (s *contextService) PrepareContext(ctx context.Context, r *v1.PrepareContextRequest) (*v1.PrepareContextResponse, error) {
	v := r.Context
	out, err := s.engine.PrepareContext(ctx, callerFrom(ctx), model.ContextRequest{Command: command(v.Command), Scope: v.Scope, Query: recallQuery(v.Query), IncludeMemories: v.IncludeMemories})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.PrepareContextResponse{Manifest: c.contextManifest(out)}
	return response, c.err
}
func (s *contextService) GetContextManifest(ctx context.Context, r *v1.GetContextManifestRequest) (*v1.GetContextManifestResponse, error) {
	out, err := s.engine.GetContextManifest(ctx, callerFrom(ctx), model.ContextQuery{ID: r.Id, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetContextManifestResponse{Manifest: c.contextManifest(out)}
	return response, c.err
}
func artifactFromProto(a *v1.ArtifactReference) *model.ArtifactReference {
	if a == nil {
		return nil
	}
	return &model.ArtifactReference{EvidenceID: a.EvidenceId, Digest: a.Digest}
}
func artifactToProto(a *model.ArtifactReference) *v1.ArtifactReference {
	if a == nil {
		return nil
	}
	return &v1.ArtifactReference{EvidenceId: a.EvidenceID, Digest: a.Digest}
}
func versionRefsFromProto(refs []*v1.VersionReference) []model.VersionReference {
	var out []model.VersionReference
	for _, r := range refs {
		out = append(out, model.VersionReference{RecordID: r.RecordId, VersionID: r.VersionId})
	}
	return out
}
func versionRefsToProto(refs []model.VersionReference) []*v1.VersionReference {
	var out []*v1.VersionReference
	for _, r := range refs {
		out = append(out, &v1.VersionReference{RecordId: r.RecordID, VersionId: r.VersionID})
	}
	return out
}
func (s *contextService) RecordDecision(ctx context.Context, r *v1.RecordDecisionRequest) (*v1.RecordDecisionResponse, error) {
	v := r.Receipt
	q := model.DecisionReceipt{Command: command(v.Command), Scope: v.Scope, ApplicationID: v.ApplicationId, ExternalDecisionID: v.ExternalDecisionId, ContextID: v.ContextId, SnapshotID: v.SnapshotId, CalculationID: v.CalculationId, SelectedItems: versionRefsFromProto(v.SelectedItems), UsedItems: versionRefsFromProto(v.UsedItems), MethodKind: v.MethodKind, MethodID: v.MethodId, MethodVersion: v.MethodVersion, TransformationVersion: v.TransformationVersion, Input: *artifactFromProto(v.Input), Output: *artifactFromProto(v.Output), ModelRequest: artifactFromProto(v.ModelRequest), ModelResponse: artifactFromProto(v.ModelResponse), ActionReceipt: artifactFromProto(v.ActionReceipt), DecidedAt: v.DecidedAt.AsTime(), Outcome: v.Outcome}
	out, err := s.engine.RecordDecision(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.RecordDecisionResponse{Record: c.decision(out)}
	return response, c.err
}
func (s *contextService) GetDecisionReceipt(ctx context.Context, r *v1.GetDecisionReceiptRequest) (*v1.GetDecisionReceiptResponse, error) {
	out, err := s.engine.GetDecisionReceipt(ctx, callerFrom(ctx), model.DecisionQuery{ID: r.Id, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetDecisionReceiptResponse{Record: c.decision(out)}
	return response, c.err
}
func commandToProto(v model.Command) *v1.Command {
	return &v1.Command{Key: v.Key, Purpose: v.Purpose, CorrelationId: v.CorrelationID}
}
func (c *responseConverter) recallQuery(q model.RecallRequest) *v1.RecallQuery {
	return &v1.RecallQuery{Scopes: q.Scopes, Query: q.Query, Purpose: q.Purpose, ValidAt: c.timestamp(q.ValidAt), KnowledgeRevision: q.KnowledgeRevision, RequireRevision: q.RequireRevision, RequireGeneration: q.RequireGeneration, Limit: int32(q.Limit)}
}
func (c *responseConverter) contextManifest(m model.ContextManifest) *v1.ContextManifest {
	r := m.Request
	out := &v1.ContextManifest{Id: m.ID, Tenant: m.Tenant, Scope: m.Scope, Caller: callerToProto(m.Caller), Request: &v1.ContextInput{Command: commandToProto(r.Command), Scope: r.Scope, Query: c.recallQuery(r.Query), IncludeMemories: r.IncludeMemories}, CanonicalVersion: m.CanonicalVersion, RedactionVersion: m.RedactionVersion, QueryReference: m.QueryReference, Retrieval: &v1.RetrievalIdentity{Generation: m.Retrieval.Generation, EmbeddingId: m.Retrieval.EmbeddingID, SearchVersion: m.Retrieval.SearchVersion, RerankerId: m.Retrieval.RerankerID, Dimensions: int64(m.Retrieval.Dimensions)}, Order: versionRefsToProto(m.Order), Rendered: m.Rendered, RenderedDigest: m.RenderedDigest, Digest: m.Digest, SelectionRevision: m.SelectionRevision, IndexedRevision: m.IndexedRevision, CreationRevision: m.CreationRevision, Incomplete: m.Incomplete}
	if a := m.Archive; a != nil {
		out.Archive = &v1.ContextArchiveReceipt{Stream: a.Stream, MaxPendingRevisions: a.MaxPendingRevisions, LedgerRevision: a.LedgerRevision, AcknowledgedRevision: a.AcknowledgedRevision, CheckedAt: c.timestamp(a.CheckedAt)}
	}
	for _, item := range m.Items {
		out.Items = append(out.Items, &v1.RecallItem{Key: keyToProto(item.Key), Fact: c.factResult(item.Fact), Score: item.Score})
	}
	for _, item := range m.Memories {
		out.Memories = append(out.Memories, &v1.MemoryRecallItem{Key: &v1.MemoryKey{Scope: item.Key.Scope, Id: item.Key.ID}, Memory: memoryResult(item.Memory), Score: item.Score})
	}
	for _, a := range m.Authorization {
		out.Authorization = append(out.Authorization, c.authorization(a))
	}
	return out
}
func (c *responseConverter) decision(d model.DecisionRecord) *v1.DecisionRecord {
	v := d.Receipt
	receipt := &v1.DecisionReceipt{Command: commandToProto(v.Command), Scope: v.Scope, ApplicationId: v.ApplicationID, ExternalDecisionId: v.ExternalDecisionID, ContextId: v.ContextID, SnapshotId: v.SnapshotID, CalculationId: v.CalculationID, SelectedItems: versionRefsToProto(v.SelectedItems), UsedItems: versionRefsToProto(v.UsedItems), MethodKind: v.MethodKind, MethodId: v.MethodID, MethodVersion: v.MethodVersion, TransformationVersion: v.TransformationVersion, Input: artifactToProto(&v.Input), Output: artifactToProto(&v.Output), ModelRequest: artifactToProto(v.ModelRequest), ModelResponse: artifactToProto(v.ModelResponse), ActionReceipt: artifactToProto(v.ActionReceipt), DecidedAt: c.timestamp(v.DecidedAt), Outcome: v.Outcome}
	out := &v1.DecisionRecord{Id: d.ID, Tenant: d.Tenant, Digest: d.Digest, SourceDigest: d.SourceDigest, CalculationDigest: d.CalculationDigest, Caller: callerToProto(d.Caller), Receipt: receipt, Scopes: d.Scopes, LedgerRevision: d.LedgerRevision}
	for _, a := range d.Authorization {
		out.Authorization = append(out.Authorization, c.authorization(a))
	}
	return out
}
