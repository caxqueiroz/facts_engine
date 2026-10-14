package connectapi

import (
	"context"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type EvidenceEngine interface {
	RegisterEvidence(context.Context, model.Caller, model.EvidenceRequest) (model.CommitResult, error)
	RegisterRedactedEvidence(context.Context, model.Caller, model.RedactedEvidenceRequest) (model.CommitResult, error)
	GetEvidenceMetadata(context.Context, model.Caller, model.EvidenceQuery) (model.Evidence, error)
	GetEvidenceContent(context.Context, model.Caller, model.EvidenceQuery) (model.EvidenceContent, error)
}
type evidenceService struct{ engine EvidenceEngine }

func objectFromProto(o *v1.ObjectRef) model.ObjectRef {
	return model.ObjectRef{Locator: o.Locator, Version: o.Version, Digest: o.Digest, Size: o.Size}
}
func objectToProto(o model.ObjectRef) *v1.ObjectRef {
	return &v1.ObjectRef{Locator: o.Locator, Version: o.Version, Digest: o.Digest, Size: o.Size}
}
func (s *evidenceService) RegisterEvidence(ctx context.Context, r *v1.RegisterEvidenceRequest) (*v1.RegisterEvidenceResponse, error) {
	e := r.Evidence
	// Derivative provenance is accepted only through its separately verified operation.
	if e.Derivation != nil {
		return nil, model.ErrInvalid
	}
	evidence := model.Evidence{ID: e.Id, Scope: e.Scope, SourceSystem: e.SourceSystem, SourceObject: e.SourceObject, SourceRevision: e.SourceRevision, Speaker: e.Speaker, SourceClass: e.SourceClass, MediaType: e.MediaType, Classification: e.Classification, RetentionPolicy: e.RetentionPolicy, SourceTime: e.SourceTime.AsTime(), Object: objectFromProto(e.Object)}
	out, err := s.engine.RegisterEvidence(ctx, callerFrom(ctx), model.EvidenceRequest{Command: command(r.Command), Evidence: evidence})
	if err != nil {
		return nil, err
	}
	return &v1.RegisterEvidenceResponse{Result: commit(out)}, nil
}
func (s *evidenceService) RegisterRedactedEvidence(ctx context.Context, r *v1.RegisterRedactedEvidenceRequest) (*v1.RegisterRedactedEvidenceResponse, error) {
	q := model.RedactedEvidenceRequest{Command: command(r.Command), ID: r.Id, Scope: r.Scope, ParentEvidenceID: r.ParentEvidenceId, PolicyVersion: r.PolicyVersion, Object: objectFromProto(r.Object)}
	for _, span := range r.Ranges {
		maximum := int64(int(^uint(0) >> 1))
		if span.OriginalStart > maximum || span.OriginalEnd > maximum || span.DerivedStart > maximum {
			return nil, model.ErrInvalid
		}
		q.Ranges = append(q.Ranges, model.EvidenceCopyRange{OriginalStart: int(span.OriginalStart), OriginalEnd: int(span.OriginalEnd), DerivedStart: int(span.DerivedStart)})
	}
	out, err := s.engine.RegisterRedactedEvidence(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	return &v1.RegisterRedactedEvidenceResponse{Result: commit(out)}, nil
}
func (s *evidenceService) GetEvidenceMetadata(ctx context.Context, r *v1.GetEvidenceMetadataRequest) (*v1.GetEvidenceMetadataResponse, error) {
	out, err := s.engine.GetEvidenceMetadata(ctx, callerFrom(ctx), model.EvidenceQuery{ID: r.Id, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetEvidenceMetadataResponse{Metadata: c.evidence(out)}
	return response, c.err
}
func (s *evidenceService) GetEvidenceContent(ctx context.Context, r *v1.GetEvidenceContentRequest) (*v1.GetEvidenceContentResponse, error) {
	out, err := s.engine.GetEvidenceContent(ctx, callerFrom(ctx), model.EvidenceQuery{ID: r.Id, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetEvidenceContentResponse{Metadata: c.evidence(out.Metadata), Content: out.Bytes}
	return response, c.err
}
func (c *responseConverter) evidence(e model.Evidence) *v1.Evidence {
	out := &v1.Evidence{Id: e.ID, Scope: e.Scope, SourceSystem: e.SourceSystem, SourceObject: e.SourceObject, SourceRevision: e.SourceRevision, Speaker: e.Speaker, SourceClass: e.SourceClass, MediaType: e.MediaType, Classification: e.Classification, RetentionPolicy: e.RetentionPolicy, SourceTime: c.timestamp(e.SourceTime), Object: objectToProto(e.Object)}
	if e.Derivation != nil {
		d := e.Derivation
		out.Derivation = &v1.EvidenceDerivation{ParentEvidenceId: d.ParentEvidenceID, TransformationVersion: d.TransformationVersion, PolicyVersion: d.PolicyVersion}
		for _, r := range d.Ranges {
			out.Derivation.Ranges = append(out.Derivation.Ranges, &v1.EvidenceCopyRange{OriginalStart: int64(r.OriginalStart), OriginalEnd: int64(r.OriginalEnd), DerivedStart: int64(r.DerivedStart)})
		}
	}
	return out
}
