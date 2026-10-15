package connectapi

import (
	"context"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type RetentionEngine interface {
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
