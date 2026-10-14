package connectapi

import (
	"context"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type CalculationEngine interface {
	RecordCalculation(context.Context, model.Caller, model.CalculationReceipt) (model.CalculationRecord, error)
	GetCalculationReceipt(context.Context, model.Caller, model.CalculationQuery) (model.CalculationRecord, error)
}
type calculationService struct{ engine CalculationEngine }

func (s *calculationService) RecordCalculation(ctx context.Context, r *v1.RecordCalculationRequest) (*v1.RecordCalculationResponse, error) {
	receipt := r.Receipt
	q := model.CalculationReceipt{Command: command(receipt.Command), Scope: receipt.Scope, SnapshotID: receipt.SnapshotId, ApplicationID: receipt.ApplicationId, ExternalRunID: receipt.ExternalRunId, MethodID: receipt.MethodId, MethodVersion: receipt.MethodVersion, ExecutableDigest: receipt.ExecutableDigest, ConfigurationDigest: receipt.ConfigurationDigest, NormalizationVersion: receipt.NormalizationVersion, ReferenceSnapshotIDs: receipt.ReferenceSnapshotIds, Input: model.ArtifactReference{EvidenceID: receipt.Input.EvidenceId, Digest: receipt.Input.Digest}, Output: model.ArtifactReference{EvidenceID: receipt.Output.EvidenceId, Digest: receipt.Output.Digest}, StartedAt: receipt.StartedAt.AsTime(), CompletedAt: receipt.CompletedAt.AsTime(), Outcome: receipt.Outcome}
	out, err := s.engine.RecordCalculation(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.RecordCalculationResponse{Record: c.calculation(out)}
	return response, c.err
}
func (s *calculationService) GetCalculationReceipt(ctx context.Context, r *v1.GetCalculationReceiptRequest) (*v1.GetCalculationReceiptResponse, error) {
	out, err := s.engine.GetCalculationReceipt(ctx, callerFrom(ctx), model.CalculationQuery{ID: r.Id, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetCalculationReceiptResponse{Record: c.calculation(out)}
	return response, c.err
}
func (c *responseConverter) calculation(r model.CalculationRecord) *v1.CalculationRecord {
	v := r.Receipt
	receipt := &v1.CalculationReceipt{Command: &v1.Command{Key: v.Key, Purpose: v.Purpose, CorrelationId: v.CorrelationID}, Scope: v.Scope, SnapshotId: v.SnapshotID, ApplicationId: v.ApplicationID, ExternalRunId: v.ExternalRunID, MethodId: v.MethodID, MethodVersion: v.MethodVersion, ExecutableDigest: v.ExecutableDigest, ConfigurationDigest: v.ConfigurationDigest, NormalizationVersion: v.NormalizationVersion, ReferenceSnapshotIds: v.ReferenceSnapshotIDs, Input: &v1.ArtifactReference{EvidenceId: v.Input.EvidenceID, Digest: v.Input.Digest}, Output: &v1.ArtifactReference{EvidenceId: v.Output.EvidenceID, Digest: v.Output.Digest}, StartedAt: c.timestamp(v.StartedAt), CompletedAt: c.timestamp(v.CompletedAt), Outcome: v.Outcome}
	out := &v1.CalculationRecord{Id: r.ID, Tenant: r.Tenant, Digest: r.Digest, SnapshotDigest: r.SnapshotDigest, Caller: callerToProto(r.Caller), Receipt: receipt, LedgerRevision: r.LedgerRevision}
	for _, a := range r.Authorization {
		out.Authorization = append(out.Authorization, c.authorization(a))
	}
	return out
}
