package connectapi

import (
	"context"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type OperationsEngine interface {
	GetProjectionStatus(context.Context, model.Caller, model.ProjectionStatusQuery) (model.ProjectionStatus, error)
	RebuildIndex(context.Context, model.Caller, model.RebuildRequest) (model.RebuildStatus, error)
}
type operationsService struct{ engine OperationsEngine }

func rebuildStatus(s model.RebuildStatus) *v1.RebuildStatus {
	return &v1.RebuildStatus{Tenant: s.Key.Tenant, Generation: s.Key.Generation, State: s.State, Fence: s.Fence, ThroughRevision: s.ThroughRevision, BaseEpoch: s.BaseEpoch, PreviousGeneration: s.PreviousGeneration, Count: int64(s.Count), Digest: s.Digest, VerificationVersion: s.VerificationVersion}
}
func (s *operationsService) RebuildIndex(ctx context.Context, r *v1.RebuildIndexRequest) (*v1.RebuildIndexResponse, error) {
	out, err := s.engine.RebuildIndex(ctx, callerFrom(ctx), model.RebuildRequest{Generation: r.Generation, Action: r.Action, Purpose: r.Purpose, MaxAttempts: int(r.MaxAttempts), ExpectedFence: r.ExpectedFence})
	if err != nil {
		return nil, err
	}
	return &v1.RebuildIndexResponse{Status: rebuildStatus(out)}, nil
}
func (s *operationsService) GetProjectionStatus(ctx context.Context, r *v1.GetProjectionStatusRequest) (*v1.GetProjectionStatusResponse, error) {
	out, err := s.engine.GetProjectionStatus(ctx, callerFrom(ctx), model.ProjectionStatusQuery{Generation: r.Generation, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	v := out.Consumer
	consumer := &v1.ProjectionConsumerStatus{Consumer: v.Key.Consumer, AcknowledgedRevision: v.AcknowledgedRevision, State: v.State, Fence: v.Fence, Attempts: int32(v.Attempts), MaxAttempts: int32(v.MaxAttempts), AvailableAt: c.timestamp(v.AvailableAt), LastFailureCode: v.LastFailureCode}
	if v.LeaseUntil != nil {
		consumer.LeaseUntil = c.timestamp(*v.LeaseUntil)
	}
	response := &v1.GetProjectionStatusResponse{ActiveGeneration: out.Active.Generation, ActiveEpoch: out.Active.Epoch, Rebuild: rebuildStatus(out.Rebuild), Consumer: consumer, LedgerRevision: out.LedgerRevision, Incomplete: out.Incomplete}
	return response, c.err
}
