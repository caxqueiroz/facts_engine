package connectapi

import (
	"context"
	"time"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type ExtractionEngine interface {
	ScanMemoryRefresh(context.Context, model.Caller, model.MemoryRefreshScan) (model.MemoryRefreshScanResult, error)
	StartMemoryRefreshSweep(context.Context, model.Caller, model.MemoryRefreshSweepRequest) (model.CommitResult, error)
	StartExtraction(context.Context, model.Caller, model.ExtractionRequest) (model.JobStatus, error)
	PlanExtractionChunks(context.Context, model.Caller, model.ExtractionChunkQuery) (model.ExtractionChunkPlan, error)
	GetJob(context.Context, model.Caller, model.JobQuery) (model.JobStatus, error)
	RetryJob(context.Context, model.Caller, model.JobRetryRequest) (model.JobStatus, error)
	GetExtractionRun(context.Context, model.Caller, model.ExtractionRunQuery) (model.ExtractionRun, error)
	StartMemoryRefresh(context.Context, model.Caller, model.MemoryRefreshRequest) (model.JobStatus, error)
}
type extractionService struct{ engine ExtractionEngine }

func inputRangesFromProto(ranges []*v1.ExtractionInputRange) ([]model.ExtractionInputRange, error) {
	var out []model.ExtractionInputRange
	for _, r := range ranges {
		if r.Start > int64(int(^uint(0)>>1)) || r.End > int64(int(^uint(0)>>1)) {
			return nil, model.ErrInvalid
		}
		out = append(out, model.ExtractionInputRange{EvidenceID: r.EvidenceId, Start: int(r.Start), End: int(r.End)})
	}
	return out, nil
}
func inputRangesToProto(ranges []model.ExtractionInputRange) []*v1.ExtractionInputRange {
	var out []*v1.ExtractionInputRange
	for _, r := range ranges {
		out = append(out, &v1.ExtractionInputRange{EvidenceId: r.EvidenceID, Start: int64(r.Start), End: int64(r.End)})
	}
	return out
}
func payloadRangesFromProto(ranges []*v1.ExtractionPayloadRange) ([]model.ExtractionPayloadRange, error) {
	var out []model.ExtractionPayloadRange
	for _, r := range ranges {
		if r.Start > int64(int(^uint(0)>>1)) || r.End > int64(int(^uint(0)>>1)) {
			return nil, model.ErrInvalid
		}
		out = append(out, model.ExtractionPayloadRange{Path: r.Path, Start: int(r.Start), End: int(r.End)})
	}
	return out, nil
}
func (s *extractionService) StartExtraction(ctx context.Context, r *v1.StartExtractionRequest) (*v1.StartExtractionResponse, error) {
	ranges, err := inputRangesFromProto(r.InputRanges)
	if err != nil {
		return nil, err
	}
	payloadRanges, err := payloadRangesFromProto(r.PayloadRanges)
	if err != nil {
		return nil, err
	}
	q := model.ExtractionRequest{Command: command(r.Command), Scope: r.Scope, ConversationID: r.ConversationId, ProfileID: r.ProfileId, ExpectedProfileDigest: r.ExpectedProfileDigest, Subjects: r.Subjects, InputRanges: ranges, PayloadRanges: payloadRanges, RedactedEvidenceIDs: r.RedactedEvidenceIds}
	for _, p := range r.Predicates {
		q.Predicates = append(q.Predicates, model.PredicateBinding{Predicate: p.Predicate, SchemaVersion: p.SchemaVersion})
	}
	for _, target := range r.MemoryTargets {
		t := model.MemoryExtractionTarget{ID: target.Id, Subtype: target.Subtype, SchemaVersion: target.SchemaVersion}
		for _, d := range target.Dependencies {
			t.Dependencies = append(t.Dependencies, model.MemoryDependency{Scope: d.Scope, RecordID: d.RecordId, VersionID: d.VersionId, ContentDigest: d.ContentDigest, Label: d.Label})
		}
		q.MemoryTargets = append(q.MemoryTargets, t)
	}
	out, err := s.engine.StartExtraction(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.StartExtractionResponse{Job: c.job(out)}
	return response, c.err
}
func (s *extractionService) PlanExtractionChunks(ctx context.Context, r *v1.PlanExtractionChunksRequest) (*v1.PlanExtractionChunksResponse, error) {
	ranges, err := inputRangesFromProto(r.InputRanges)
	if err != nil {
		return nil, err
	}
	q := model.ExtractionChunkQuery{Scope: r.Scope, ConversationID: r.ConversationId, ProfileID: r.ProfileId, Purpose: r.Purpose, MaxSourceBytes: int(r.MaxSourceBytes), InputRanges: ranges, RedactedEvidenceIDs: r.RedactedEvidenceIds}
	out, err := s.engine.PlanExtractionChunks(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	plan := &v1.ExtractionChunkPlan{Version: out.Version, Tenant: out.Tenant, Scope: out.Scope, ConversationId: out.ConversationID, ConversationDigest: out.ConversationDigest, ProfileId: out.ProfileID, ProfileDigest: out.ProfileDigest, MaxSourceBytes: int64(out.MaxSourceBytes), ExcludedRanges: inputRangesToProto(out.ExcludedRanges), RedactedEvidenceIds: out.RedactedEvidenceIDs}
	for _, chunk := range out.Chunks {
		plan.Chunks = append(plan.Chunks, &v1.ExtractionChunk{InputRanges: inputRangesToProto(chunk.InputRanges), SourceBytes: int64(chunk.SourceBytes)})
	}
	return &v1.PlanExtractionChunksResponse{Plan: plan}, nil
}
func (s *extractionService) GetJob(ctx context.Context, r *v1.GetJobRequest) (*v1.GetJobResponse, error) {
	out, err := s.engine.GetJob(ctx, callerFrom(ctx), model.JobQuery{ID: r.Id, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetJobResponse{Job: c.job(out)}
	return response, c.err
}
func (s *extractionService) RetryJob(ctx context.Context, r *v1.RetryJobRequest) (*v1.RetryJobResponse, error) {
	out, err := s.engine.RetryJob(ctx, callerFrom(ctx), model.JobRetryRequest{Command: command(r.Command), ID: r.Id, Scope: r.Scope, ExpectedFence: r.ExpectedFence})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.RetryJobResponse{Job: c.job(out)}
	return response, c.err
}
func (s *extractionService) StartMemoryRefresh(ctx context.Context, r *v1.StartMemoryRefreshRequest) (*v1.StartMemoryRefreshResponse, error) {
	payloadRanges, err := payloadRangesFromProto(r.PayloadRanges)
	if err != nil {
		return nil, err
	}
	q := model.MemoryRefreshRequest{Command: command(r.Command), Scope: r.Scope, RecordID: r.RecordId, VersionID: r.VersionId, ProfileID: r.ProfileId, PayloadRanges: payloadRanges}
	if r.Trigger != nil {
		q.Trigger = &model.MemoryRefreshTrigger{EventID: r.Trigger.EventId, LedgerRevision: r.Trigger.LedgerRevision}
	}
	out, err := s.engine.StartMemoryRefresh(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.StartMemoryRefreshResponse{Job: c.job(out)}
	return response, c.err
}
func (s *extractionService) GetExtractionRun(ctx context.Context, r *v1.GetExtractionRunRequest) (*v1.GetExtractionRunResponse, error) {
	out, err := s.engine.GetExtractionRun(ctx, callerFrom(ctx), model.ExtractionRunQuery{ID: r.Id, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	run := &v1.ExtractionRun{Attempt: c.extractionAttempt(out.Attempt)}
	if out.Result != nil {
		run.Result = c.extractionResult(*out.Result)
	}
	return &v1.GetExtractionRunResponse{Run: run}, c.err
}
func (c *responseConverter) job(j model.JobStatus) *v1.JobStatus {
	out := &v1.JobStatus{Id: j.ID, Scope: j.Scope, Kind: j.Kind, State: j.State, AttemptId: j.AttemptID, LastFailureCode: j.LastFailureCode, Purpose: j.Purpose, Attempts: int64(j.Attempts), MaxAttempts: int64(j.MaxAttempts), Fence: j.Fence, CreatedRevision: j.CreatedRevision, AvailableAt: c.timestamp(j.AvailableAt)}
	if j.LeaseUntil != nil {
		out.LeaseUntil = c.timestamp(*j.LeaseUntil)
	}
	return out
}
func (c *responseConverter) extractionAttempt(a model.ExtractionAttempt) *v1.ExtractionAttempt {
	out := &v1.ExtractionAttempt{Id: a.ID, Tenant: a.Tenant, Scope: a.Scope, JobId: a.JobID, ProfileDigest: a.ProfileDigest, RequestDigest: a.RequestDigest, Caller: callerToProto(a.Caller), Fence: a.Fence, LedgerRevision: a.LedgerRevision, LeaseUntil: c.timestamp(a.LeaseUntil), Number: int64(a.Number), StartedAt: c.timestamp(a.StartedAt), RequestBytes: a.RequestBytes, ContentScopes: a.ContentScopes, RefreshScopes: a.RefreshScopes}
	for _, receipt := range a.Authorization {
		out.Authorization = append(out.Authorization, c.authorization(receipt))
	}
	if a.Reservation != nil {
		r := a.Reservation
		out.Reservation = &v1.ExtractionReservation{Day: r.Day, Tokens: r.Tokens, DailyTokenLimit: r.DailyTokenLimit, ConcurrentLimit: int64(r.ConcurrentLimit), CostUnit: r.CostUnit, CostUnits: r.CostUnits, DailyCostLimit: r.DailyCostLimit}
	}
	return out
}
func (c *responseConverter) extractionResult(r model.ExtractionResult) *v1.ExtractionResult {
	out := &v1.ExtractionResult{AttemptId: r.AttemptID, JobId: r.JobID, Tenant: r.Tenant, Scope: r.Scope, Outcome: r.Outcome, ModelIdentity: r.ModelIdentity, RequestDigest: r.RequestDigest, ResponseDigest: r.ResponseDigest, ResponseBytes: r.ResponseBytes, ResponseComplete: r.ResponseComplete, InputTokens: r.InputTokens, OutputTokens: r.OutputTokens, CompletedAt: c.timestamp(r.CompletedAt), LedgerRevision: r.LedgerRevision, ContentScopes: r.ContentScopes, RefreshScopes: r.RefreshScopes}
	for _, i := range r.Issues {
		out.Issues = append(out.Issues, &v1.ExtractionIssue{Candidate: int64(i.Candidate), Code: i.Code})
	}
	for _, relation := range r.Relations {
		out.Relations = append(out.Relations, &v1.ExtractionRelation{Candidate: int64(relation.Candidate), RecordId: relation.RecordID, VersionId: relation.VersionID, Kind: relation.Kind})
	}
	for _, p := range r.Proposals {
		out.Proposals = append(out.Proposals, &v1.ExtractedProposal{Candidate: int64(p.Candidate), RecordId: p.RecordID, VersionId: p.VersionID, RecordKind: p.RecordKind})
	}
	for _, a := range r.Authorization {
		out.Authorization = append(out.Authorization, c.authorization(a))
	}
	return out
}

func (s *extractionService) ScanMemoryRefresh(ctx context.Context, r *v1.ScanMemoryRefreshRequest) (*v1.ScanMemoryRefreshResponse, error) {
	out, err := s.engine.ScanMemoryRefresh(ctx, callerFrom(ctx), model.MemoryRefreshScan{Scope: r.Scope, Purpose: r.Purpose, Trigger: model.MemoryRefreshTrigger{EventID: r.Trigger.EventId, LedgerRevision: r.Trigger.LedgerRevision}, AfterVersionID: r.AfterVersionId, Limit: int(r.Limit)})
	if err != nil {
		return nil, err
	}
	response := &v1.ScanMemoryRefreshResponse{}
	for _, version := range out.Versions {
		response.Versions = append(response.Versions, &v1.VersionReference{RecordId: version.RecordID, VersionId: version.VersionID})
	}
	return response, nil
}
func (s *extractionService) StartMemoryRefreshSweep(ctx context.Context, r *v1.StartMemoryRefreshSweepRequest) (*v1.StartMemoryRefreshSweepResponse, error) {
	out, err := s.engine.StartMemoryRefreshSweep(ctx, callerFrom(ctx), model.MemoryRefreshSweepRequest{Command: command(r.Command), Scope: r.Scope, Stream: r.Stream, ProfileID: r.ProfileId, ScheduledFor: r.ScheduledFor.AsTime(), Interval: time.Duration(r.IntervalMinutes) * time.Minute})
	if err != nil {
		return nil, err
	}
	return &v1.StartMemoryRefreshSweepResponse{Result: commit(out)}, nil
}
