package connectapi

import (
	"context"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type RetentionEngine interface {
	DisposeBatch(context.Context, model.Caller, model.DisposalBatchRequest) (model.CommitResult, error)
	DisposeAttestation(context.Context, model.Caller, model.AttestationDisposalRequest) (model.CommitResult, error)
	DisposeConversation(context.Context, model.Caller, model.ConversationDisposalRequest) (model.CommitResult, error)
	CleanupProjection(context.Context, model.Caller, model.ProjectionCleanupRequest) (model.CommitResult, error)
	DisposeVersion(context.Context, model.Caller, model.VersionDisposalRequest) (model.CommitResult, error)
	DisposeExtraction(context.Context, model.Caller, model.ExtractionDisposalRequest) (model.CommitResult, error)
	DisposeContext(context.Context, model.Caller, model.ContextDisposalRequest) (model.CommitResult, error)
	DisposeSnapshot(context.Context, model.Caller, model.SnapshotDisposalRequest) (model.CommitResult, error)
	DisposeOrphanEvidence(context.Context, model.Caller, model.OrphanEvidenceDisposalRequest) (model.CommitResult, error)
	RegisterRetentionPolicy(context.Context, model.Caller, model.RetentionPolicyRequest) (model.CommitResult, error)
	PlaceLegalHold(context.Context, model.Caller, model.LegalHoldRequest) (model.CommitResult, error)
	ReleaseLegalHold(context.Context, model.Caller, model.ReleaseLegalHoldRequest) (model.CommitResult, error)
	DisposeEvidence(context.Context, model.Caller, model.EvidenceDisposalRequest) (model.CommitResult, error)
	GetRetentionStatus(context.Context, model.Caller, model.RetentionStatusQuery) (model.RetentionStatus, error)
}

func (s *retentionService) DisposeAttestation(ctx context.Context, r *v1.DisposeAttestationRequest) (*v1.DisposeAttestationResponse, error) {
	out, err := s.engine.DisposeAttestation(ctx, callerFrom(ctx), model.AttestationDisposalRequest{Command: command(r.Command), Kind: r.Kind, Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, AttestationID: r.AttestationId, PolicyID: r.PolicyId, Jurisdiction: r.Jurisdiction, Purpose: r.Purpose, Reason: r.Reason})
	if err != nil {
		return nil, err
	}
	return &v1.DisposeAttestationResponse{Result: commit(out)}, nil
}

func (s *retentionService) DisposeConversation(ctx context.Context, r *v1.DisposeConversationRequest) (*v1.DisposeConversationResponse, error) {
	out, err := s.engine.DisposeConversation(ctx, callerFrom(ctx), model.ConversationDisposalRequest{Command: command(r.Command), Scope: r.Scope, ConversationID: r.ConversationId, PolicyID: r.PolicyId, Jurisdiction: r.Jurisdiction, Purpose: r.Purpose, Reason: r.Reason})
	if err != nil {
		return nil, err
	}
	return &v1.DisposeConversationResponse{Result: commit(out)}, nil
}

type retentionService struct{ engine RetentionEngine }

func (s *retentionService) RegisterRetentionPolicy(ctx context.Context, r *v1.RegisterRetentionPolicyRequest) (*v1.RegisterRetentionPolicyResponse, error) {
	p := r.Policy
	out, err := s.engine.RegisterRetentionPolicy(ctx, callerFrom(ctx), model.RetentionPolicyRequest{Command: command(r.Command), Policy: model.RetentionPolicy{ID: p.Id, Scope: p.Scope, RecordClass: p.RecordClass, Purpose: p.Purpose, Jurisdiction: p.Jurisdiction, MinimumAgeSeconds: p.MinimumAgeSeconds}})
	if err != nil {
		return nil, err
	}
	return &v1.RegisterRetentionPolicyResponse{Result: commit(out)}, nil
}
func (s *retentionService) PlaceLegalHold(ctx context.Context, r *v1.PlaceLegalHoldRequest) (*v1.PlaceLegalHoldResponse, error) {
	out, err := s.engine.PlaceLegalHold(ctx, callerFrom(ctx), model.LegalHoldRequest{Command: command(r.Command), ID: r.Id, Scope: r.Scope, Reason: r.Reason})
	if err != nil {
		return nil, err
	}
	return &v1.PlaceLegalHoldResponse{Result: commit(out)}, nil
}
func (s *retentionService) ReleaseLegalHold(ctx context.Context, r *v1.ReleaseLegalHoldRequest) (*v1.ReleaseLegalHoldResponse, error) {
	out, err := s.engine.ReleaseLegalHold(ctx, callerFrom(ctx), model.ReleaseLegalHoldRequest{Command: command(r.Command), ID: r.Id, Scope: r.Scope, Reason: r.Reason})
	if err != nil {
		return nil, err
	}
	return &v1.ReleaseLegalHoldResponse{Result: commit(out)}, nil
}
func (s *retentionService) DisposeEvidence(ctx context.Context, r *v1.DisposeEvidenceRequest) (*v1.DisposeEvidenceResponse, error) {
	out, err := s.engine.DisposeEvidence(ctx, callerFrom(ctx), model.EvidenceDisposalRequest{Command: command(r.Command), Scope: r.Scope, EvidenceID: r.EvidenceId, DerivativeEvidenceIDs: append([]string(nil), r.DerivativeEvidenceIds...), PolicyID: r.PolicyId, Jurisdiction: r.Jurisdiction, Reason: r.Reason})
	if err != nil {
		return nil, err
	}
	return &v1.DisposeEvidenceResponse{Result: commit(out)}, nil
}
func (s *retentionService) GetRetentionStatus(ctx context.Context, r *v1.GetRetentionStatusRequest) (*v1.GetRetentionStatusResponse, error) {
	out, err := s.engine.GetRetentionStatus(ctx, callerFrom(ctx), model.RetentionStatusQuery{Scope: r.Scope, EvidenceID: r.EvidenceId, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	converter := responseConverter{}
	return &v1.GetRetentionStatusResponse{Status: &v1.RetentionStatus{EvidenceId: out.EvidenceID, PolicyId: out.PolicyID, RegisteredAt: converter.timestamp(out.RegisteredAt), Disposed: out.Disposed, ActiveHolds: append([]string(nil), out.ActiveHolds...), Blockers: append([]string(nil), out.Blockers...)}}, converter.err
}

func (s *retentionService) DisposeOrphanEvidence(ctx context.Context, r *v1.DisposeOrphanEvidenceRequest) (*v1.DisposeOrphanEvidenceResponse, error) {
	out, err := s.engine.DisposeOrphanEvidence(ctx, callerFrom(ctx), model.OrphanEvidenceDisposalRequest{Command: command(r.Command), Scope: r.Scope, PolicyID: r.PolicyId, Jurisdiction: r.Jurisdiction, Reason: r.Reason, Object: objectFromProto(r.Object)})
	if err != nil {
		return nil, err
	}
	return &v1.DisposeOrphanEvidenceResponse{Result: commit(out)}, nil
}

func (s *retentionService) DisposeSnapshot(ctx context.Context, r *v1.DisposeSnapshotRequest) (*v1.DisposeSnapshotResponse, error) {
	request := model.SnapshotDisposalRequest{Command: command(r.Command), SnapshotID: r.SnapshotId, Reason: r.Reason}
	for _, b := range r.Bindings {
		request.Bindings = append(request.Bindings, model.SnapshotRetentionBinding{Scope: b.Scope, PolicyID: b.PolicyId, Jurisdiction: b.Jurisdiction})
	}
	out, err := s.engine.DisposeSnapshot(ctx, callerFrom(ctx), request)
	if err != nil {
		return nil, err
	}
	return &v1.DisposeSnapshotResponse{Result: commit(out)}, nil
}

func (s *retentionService) DisposeContext(ctx context.Context, r *v1.DisposeContextRequest) (*v1.DisposeContextResponse, error) {
	request := model.ContextDisposalRequest{Command: command(r.Command), ContextID: r.ContextId, Reason: r.Reason}
	for _, b := range r.Bindings {
		request.Bindings = append(request.Bindings, model.ContextRetentionBinding{Scope: b.Scope, PolicyID: b.PolicyId, Jurisdiction: b.Jurisdiction})
	}
	out, err := s.engine.DisposeContext(ctx, callerFrom(ctx), request)
	if err != nil {
		return nil, err
	}
	return &v1.DisposeContextResponse{Result: commit(out)}, nil
}

func (s *retentionService) DisposeExtraction(ctx context.Context, r *v1.DisposeExtractionRequest) (*v1.DisposeExtractionResponse, error) {
	request := model.ExtractionDisposalRequest{Command: command(r.Command), JobID: r.JobId, Reason: r.Reason}
	for _, b := range r.Bindings {
		request.Bindings = append(request.Bindings, model.ExtractionRetentionBinding{Scope: b.Scope, JobPolicyID: b.JobPolicyId, RequestPolicyID: b.RequestPolicyId, ResponsePolicyID: b.ResponsePolicyId, Jurisdiction: b.Jurisdiction})
	}
	out, err := s.engine.DisposeExtraction(ctx, callerFrom(ctx), request)
	if err != nil {
		return nil, err
	}
	return &v1.DisposeExtractionResponse{Result: commit(out)}, nil
}

func (s *retentionService) DisposeVersion(ctx context.Context, r *v1.DisposeVersionRequest) (*v1.DisposeVersionResponse, error) {
	request := model.VersionDisposalRequest{Command: command(r.Command), VersionID: r.VersionId, Kind: r.Kind, Scope: r.Scope, RecordID: r.RecordId, Reason: r.Reason}
	for _, b := range r.Bindings {
		request.Bindings = append(request.Bindings, model.VersionRetentionBinding{Scope: b.Scope, PolicyID: b.PolicyId, Jurisdiction: b.Jurisdiction})
	}
	out, err := s.engine.DisposeVersion(ctx, callerFrom(ctx), request)
	if err != nil {
		return nil, err
	}
	return &v1.DisposeVersionResponse{Result: commit(out)}, nil
}

func (s *retentionService) CleanupProjection(ctx context.Context, r *v1.CleanupProjectionRequest) (*v1.CleanupProjectionResponse, error) {
	request := model.ProjectionCleanupRequest{Command: command(r.Command), VersionID: r.VersionId, Kind: r.Kind, Scope: r.Scope, RecordID: r.RecordId, Reason: r.Reason, DisposalEventID: r.DisposalEventId, Generation: r.Generation}
	for _, b := range r.Bindings {
		request.Bindings = append(request.Bindings, model.VersionRetentionBinding{Scope: b.Scope, PolicyID: b.PolicyId, Jurisdiction: b.Jurisdiction})
	}
	out, err := s.engine.CleanupProjection(ctx, callerFrom(ctx), request)
	if err != nil {
		return nil, err
	}
	return &v1.CleanupProjectionResponse{Result: commit(out)}, nil
}

func (s *retentionService) DisposeBatch(ctx context.Context, r *v1.DisposeBatchRequest) (*v1.DisposeBatchResponse, error) {
	request := model.DisposalBatchRequest{Command: command(r.Command), Scope: r.Scope, Reason: r.Reason}
	for _, v := range r.Versions {
		selection := model.VersionDisposalRequest{Kind: v.Kind, Scope: v.Scope, RecordID: v.RecordId, VersionID: v.VersionId}
		for _, b := range v.Bindings {
			selection.Bindings = append(selection.Bindings, model.VersionRetentionBinding{Scope: b.Scope, PolicyID: b.PolicyId, Jurisdiction: b.Jurisdiction})
		}
		request.Versions = append(request.Versions, selection)
	}
	for _, e := range r.Extractions {
		selection := model.ExtractionDisposalRequest{JobID: e.JobId}
		for _, b := range e.Bindings {
			selection.Bindings = append(selection.Bindings, model.ExtractionRetentionBinding{Scope: b.Scope, JobPolicyID: b.JobPolicyId, RequestPolicyID: b.RequestPolicyId, ResponsePolicyID: b.ResponsePolicyId, Jurisdiction: b.Jurisdiction})
		}
		request.Extractions = append(request.Extractions, selection)
	}
	for _, item := range r.Conversations {
		request.Conversations = append(request.Conversations, model.ConversationDisposalRequest{Scope: item.Scope, ConversationID: item.ConversationId, PolicyID: item.PolicyId, Jurisdiction: item.Jurisdiction, Purpose: item.Purpose})
	}
	for _, item := range r.Snapshots {
		selection := model.SnapshotDisposalRequest{SnapshotID: item.SnapshotId}
		for _, b := range item.Bindings {
			selection.Bindings = append(selection.Bindings, model.SnapshotRetentionBinding{Scope: b.Scope, PolicyID: b.PolicyId, Jurisdiction: b.Jurisdiction})
		}
		request.Snapshots = append(request.Snapshots, selection)
	}
	for _, item := range r.Contexts {
		selection := model.ContextDisposalRequest{ContextID: item.ContextId}
		for _, b := range item.Bindings {
			selection.Bindings = append(selection.Bindings, model.ContextRetentionBinding{Scope: b.Scope, PolicyID: b.PolicyId, Jurisdiction: b.Jurisdiction})
		}
		request.Contexts = append(request.Contexts, selection)
	}
	for _, item := range r.Calculations {
		selection := model.CalculationDisposalRequest{CalculationID: item.CalculationId}
		for _, b := range item.Bindings {
			selection.Bindings = append(selection.Bindings, model.CalculationRetentionBinding{Scope: b.Scope, PolicyID: b.PolicyId, Jurisdiction: b.Jurisdiction})
		}
		request.Calculations = append(request.Calculations, selection)
	}
	for _, item := range r.Decisions {
		selection := model.DecisionDisposalRequest{DecisionID: item.DecisionId}
		for _, b := range item.Bindings {
			selection.Bindings = append(selection.Bindings, model.DecisionRetentionBinding{Scope: b.Scope, PolicyID: b.PolicyId, Jurisdiction: b.Jurisdiction})
		}
		request.Decisions = append(request.Decisions, selection)
	}
	for _, item := range r.Attestations {
		request.Attestations = append(request.Attestations, model.AttestationDisposalRequest{Command: request.Command, Reason: request.Reason, Kind: item.Kind, Scope: item.Scope, RecordID: item.RecordId, VersionID: item.VersionId, AttestationID: item.AttestationId, PolicyID: item.PolicyId, Jurisdiction: item.Jurisdiction, Purpose: item.Purpose})
	}
	for _, item := range r.Evidence {
		request.Evidence = append(request.Evidence, model.EvidenceDisposalRequest{Command: request.Command, Reason: request.Reason, Scope: item.Scope, EvidenceID: item.EvidenceId, PolicyID: item.PolicyId, Jurisdiction: item.Jurisdiction, DerivativeEvidenceIDs: append([]string(nil), item.DerivativeEvidenceIds...)})
	}
	result, err := s.engine.DisposeBatch(ctx, callerFrom(ctx), request)
	if err != nil {
		return nil, err
	}
	return &v1.DisposeBatchResponse{Result: commit(result)}, nil
}
