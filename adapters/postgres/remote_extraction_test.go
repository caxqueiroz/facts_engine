package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testRemoteExtraction(t *testing.T, ctx context.Context, ledger model.Ledger, store *evidence.Postgres) {
	t.Helper()
	caller := model.Caller{Tenant: "remote-extraction", Principal: "author"}
	worker := model.Caller{Tenant: caller.Tenant, Principal: "worker", Service: true}
	at := time.Now().UTC().Truncate(time.Microsecond)
	prompt := []byte("Return evidence-backed candidates only.")
	profile := model.ExtractionProfile{ID: "approved", ProviderID: "fixture", RequestedModel: "fixture-v1", PromptVersion: "v1", PromptDigest: model.BytesDigest(prompt), Prompt: prompt, Parameters: json.RawMessage("{}"), ParserVersion: "facts-extraction-json-v1", InputSelectionVersion: "original-ranges-v1", ApplicationBuild: "test", Timeout: time.Second, MaxInputBytes: 1 << 20, MaxOutputBytes: 1 << 20, MaxCandidates: 8, MaxAttempts: 1, MaxTokensPerAttempt: 1000, RetryDelay: time.Second}
	var calls atomic.Int32
	provider := extractionFunc(func(_ context.Context, raw []byte) (model.ExtractionResponse, error) {
		if calls.Add(1) == 1 {
			return model.ExtractionResponse{}, model.ErrUnavailable
		}
		var input model.ExtractionInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return model.ExtractionResponse{}, err
		}
		text := "tea"
		body, err := json.Marshal(model.ExtractionOutput{Candidates: []model.ExtractionCandidate{{Subject: "subject", Predicate: "preference", SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &text}, Valid: model.Interval{From: &at}, Sources: []model.SourceSpan{{EvidenceID: input.Messages[0].EvidenceID, Start: 0, End: 3, Quote: "tea"}}, Uncertainty: "asserted", Rationale: "explicit source", Confidence: 1}}})
		return model.ExtractionResponse{ModelIdentity: "fixture-v1", Body: body, InputTokens: 10, OutputTokens: 10}, err
	})
	options := model.EngineOptions{Extraction: model.ExtractionOptions{Profiles: []model.ApprovedExtractor{{Profile: profile, Provider: provider}}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 10000}}
	engine, err := facts.NewWithOptions(ledger, allow{}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	registerTestIdentities(t, ctx, engine, caller.Tenant, "case", []string{"subject"})
	command := func(key string) model.Command { return model.Command{Key: key, Purpose: "extract"} }
	if _, err := engine.RegisterAcceptancePolicy(ctx, caller, model.PolicyRequest{Command: command("policy"), Policy: model.AcceptancePolicy{ID: "policy", Scope: "case", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RegisterPredicateVersion(ctx, caller, model.PredicateRequest{Command: command("schema"), Definition: model.PredicateVersion{Scope: "case", Predicate: "preference", Version: "v1", PolicyID: "policy", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	object, err := store.Put(ctx, caller.Tenant, "message", []byte("tea"))
	if err != nil {
		t.Fatal(err)
	}
	rpc := governanceClient(t, engine, caller)
	ingestion := memoryv1connect.NewIngestionServiceClient(rpc)
	extraction := memoryv1connect.NewExtractionServiceClient(rpc)
	request := &v1.IngestConversationRequest{Tenant: caller.Tenant, Conversation: &v1.ConversationInput{Command: &v1.Command{Key: "conversation", Purpose: "extract"}, Scope: "case", SourceSystem: "chat", ConversationId: "conversation", Revision: "v1", Timezone: "UTC", Messages: []*v1.ConversationMessage{{Id: "message", Revision: "v1", Speaker: "customer", SpeakerRole: "customer", SourceClass: "customer", SourceTime: timestamppb.New(at), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "retain", Object: &v1.ObjectRef{Locator: object.Locator, Version: object.Version, Digest: object.Digest, Size: object.Size}}}}}
	ingested, err := ingestion.IngestConversation(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	embedded := model.ConversationRequest{Command: command("conversation"), Scope: "case", SourceSystem: "chat", ConversationID: "conversation", Revision: "v1", Timezone: "UTC", Messages: []model.ConversationMessage{{ID: "message", Revision: "v1", Speaker: "customer", SpeakerRole: "customer", SourceClass: "customer", SourceTime: at, MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "retain", Object: object}}}
	again, err := engine.IngestConversation(ctx, caller, embedded)
	if err != nil || again.ID != ingested.Conversation.Id || again.Digest != ingested.Conversation.Digest || again.LedgerRevision != ingested.Conversation.LedgerRevision || !proto.Equal(ingested.Conversation.Request, request.Conversation) {
		t.Fatalf("conversation replay: %+v, %v", again, err)
	}
	fetched, err := ingestion.GetConversation(ctx, &v1.GetConversationRequest{Tenant: caller.Tenant, Scope: "case", Id: again.ID, Purpose: "extract"})
	if err != nil || !proto.Equal(fetched.Conversation, ingested.Conversation) {
		t.Fatalf("conversation read: %v, %v", fetched, err)
	}
	plan, err := extraction.PlanExtractionChunks(ctx, &v1.PlanExtractionChunksRequest{Tenant: caller.Tenant, Scope: "case", ConversationId: again.ID, ProfileId: profile.ID, Purpose: "extract", MaxSourceBytes: 4})
	if err != nil || len(plan.Plan.Chunks) != 1 || plan.Plan.Chunks[0].SourceBytes != 3 || plan.Plan.ProfileDigest != model.ExtractionProfileDigest(profile) || calls.Load() != 0 {
		t.Fatalf("chunk planning: %v, %v", plan, err)
	}
	start := &v1.StartExtractionRequest{Tenant: caller.Tenant, Command: &v1.Command{Key: "start", Purpose: "extract"}, Scope: "case", ConversationId: again.ID, ProfileId: profile.ID, Subjects: []string{"subject"}, Predicates: []*v1.PredicateBinding{{Predicate: "preference", SchemaVersion: "v1"}}, InputRanges: plan.Plan.Chunks[0].InputRanges, ExpectedProfileDigest: plan.Plan.ProfileDigest}
	job, err := extraction.StartExtraction(ctx, start)
	if err != nil || job.Job.State != "pending" || calls.Load() != 0 {
		t.Fatalf("job start invoked provider: %v, %v", job, err)
	}
	input := model.ExtractionRequest{Command: command("start"), Scope: "case", ConversationID: again.ID, ProfileID: profile.ID, Subjects: []string{"subject"}, Predicates: []model.PredicateBinding{{Predicate: "preference", SchemaVersion: "v1"}}, InputRanges: []model.ExtractionInputRange{{EvidenceID: again.EvidenceIDs[0], Start: 0, End: 3}}, ExpectedProfileDigest: plan.Plan.ProfileDigest}
	duplicate, err := engine.StartExtraction(ctx, caller, input)
	if err != nil || duplicate.ID != job.Job.Id || duplicate.CreatedRevision != job.Job.CreatedRevision {
		t.Fatalf("job replay: %+v, %v", duplicate, err)
	}
	failed, err := engine.RunExtraction(ctx, worker, model.ExtractionRunRequest{Command: command("run-fail"), ID: job.Job.Id, Scope: "case"})
	if err != nil || failed.State != "failed" || calls.Load() != 1 {
		t.Fatalf("provider failure: %+v, %v", failed, err)
	}
	status, err := extraction.GetJob(ctx, &v1.GetJobRequest{Tenant: caller.Tenant, Scope: "case", Id: job.Job.Id, Purpose: "extract"})
	if err != nil || status.Job.State != "failed" || status.Job.LastFailureCode != "provider_unavailable" || status.Job.Fence != failed.Fence {
		t.Fatalf("job status: %v, %v", status, err)
	}
	denied, err := facts.NewWithOptions(ledger, denyPermission{permission: "can_operate"}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	deniedClient := memoryv1connect.NewExtractionServiceClient(governanceClient(t, denied, caller))
	retryRequest := &v1.RetryJobRequest{Tenant: caller.Tenant, Command: &v1.Command{Key: "retry", Purpose: "extract"}, Scope: "case", Id: job.Job.Id, ExpectedFence: failed.Fence}
	if _, err := deniedClient.RetryJob(ctx, retryRequest); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("retry authorization: %v", err)
	}
	retried, err := extraction.RetryJob(ctx, retryRequest)
	if err != nil || retried.Job.State != "pending" || retried.Job.Fence <= failed.Fence {
		t.Fatalf("audited retry: %v, %v", retried, err)
	}
	retryRequest.Command.Key = "stale-retry"
	if _, err := extraction.RetryJob(ctx, retryRequest); connect.CodeOf(err) != connect.CodeAborted {
		t.Fatalf("stale retry fence: %v", err)
	}
	completed, err := engine.RunExtraction(ctx, worker, model.ExtractionRunRequest{Command: command("run-success"), ID: job.Job.Id, Scope: "case"})
	if err != nil || completed.State != "completed" || calls.Load() != 2 {
		t.Fatalf("completed job: %+v, %v", completed, err)
	}
	run, err := extraction.GetExtractionRun(ctx, &v1.GetExtractionRunRequest{Tenant: caller.Tenant, Scope: "case", Id: completed.AttemptID, Purpose: "extract"})
	if err != nil || run.Run.Result == nil || len(run.Run.Result.Proposals) != 1 || !run.Run.Result.ResponseComplete || model.BytesDigest(run.Run.Attempt.RequestBytes) != run.Run.Attempt.RequestDigest || model.BytesDigest(run.Run.Result.ResponseBytes) != run.Run.Result.ResponseDigest {
		t.Fatalf("retained run: %v, %v", run, err)
	}
	stored, err := engine.GetExtractionRun(ctx, caller, model.ExtractionRunQuery{ID: completed.AttemptID, Scope: "case", Purpose: "extract"})
	if err != nil || !bytes.Equal(stored.Attempt.RequestBytes, run.Run.Attempt.RequestBytes) || !bytes.Equal(stored.Result.ResponseBytes, run.Run.Result.ResponseBytes) || stored.Result.Proposals[0].VersionID != run.Run.Result.Proposals[0].VersionId || run.Run.Attempt.Reservation.Tokens != profile.MaxTokensPerAttempt {
		t.Fatalf("run parity: %+v, %v", stored, err)
	}
	fact, err := engine.GetFact(ctx, caller, model.FactQuery{Purpose: "extract", Key: model.FactKey{Scope: "case", Subject: "subject", Predicate: "preference"}, ValidAt: at})
	if err != nil || fact.State != "unresolved" {
		t.Fatalf("extraction auto-accepted candidate: %+v, %v", fact, err)
	}
}

func testRemoteExtractionReplay(t *testing.T, ctx context.Context, engine *facts.Engine, caller model.Caller, r model.ExtractionRequest, job model.JobStatus) {
	t.Helper()
	client := memoryv1connect.NewExtractionServiceClient(governanceClient(t, engine, caller))
	request := &v1.StartExtractionRequest{Tenant: caller.Tenant, Command: &v1.Command{Key: r.Key, Purpose: r.Purpose, CorrelationId: r.CorrelationID}, Scope: r.Scope, ConversationId: r.ConversationID, ProfileId: r.ProfileID, ExpectedProfileDigest: r.ExpectedProfileDigest, Subjects: r.Subjects, RedactedEvidenceIds: r.RedactedEvidenceIDs}
	for _, p := range r.Predicates {
		request.Predicates = append(request.Predicates, &v1.PredicateBinding{Predicate: p.Predicate, SchemaVersion: p.SchemaVersion})
	}
	for _, span := range r.InputRanges {
		request.InputRanges = append(request.InputRanges, &v1.ExtractionInputRange{EvidenceId: span.EvidenceID, Start: int64(span.Start), End: int64(span.End)})
	}
	for _, target := range r.MemoryTargets {
		out := &v1.MemoryExtractionTarget{Id: target.ID, Subtype: target.Subtype, SchemaVersion: target.SchemaVersion}
		for _, d := range target.Dependencies {
			out.Dependencies = append(out.Dependencies, &v1.MemoryDependency{Scope: d.Scope, RecordId: d.RecordID, VersionId: d.VersionID, ContentDigest: d.ContentDigest, Label: d.Label})
		}
		request.MemoryTargets = append(request.MemoryTargets, out)
	}
	got, err := client.StartExtraction(ctx, request)
	if err != nil || got.Job.Id != job.ID || got.Job.CreatedRevision != job.CreatedRevision {
		t.Fatalf("remote extraction replay: %v, %v", got, err)
	}
}
func testRemoteRefreshReplay(t *testing.T, ctx context.Context, engine *facts.Engine, ledger model.Ledger, caller model.Caller, status model.JobStatus) {
	t.Helper()
	var job model.ExtractionJob
	err := ledger.Transact(ctx, caller.Tenant, func(tx model.Tx) error {
		var err error
		job, err = tx.(model.ExtractionTx).ExtractionJob(ctx, status.ID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	r := job.Request
	if r.Refresh == nil {
		t.Fatal("expected refresh job")
	}
	request := &v1.StartMemoryRefreshRequest{Tenant: job.Caller.Tenant, Command: &v1.Command{Key: r.Key, Purpose: r.Purpose, CorrelationId: r.CorrelationID}, Scope: r.Scope, RecordId: r.Refresh.RecordID, VersionId: r.Refresh.VersionID, ProfileId: r.ProfileID}
	if r.RefreshTrigger != nil {
		request.Trigger = &v1.MemoryRefreshTrigger{EventId: r.RefreshTrigger.EventID, LedgerRevision: r.RefreshTrigger.LedgerRevision}
	}
	client := memoryv1connect.NewExtractionServiceClient(governanceClient(t, engine, job.Caller))
	got, err := client.StartMemoryRefresh(ctx, request)
	if err != nil || got.Job.Id != status.ID || got.Job.CreatedRevision != status.CreatedRevision {
		t.Fatalf("remote refresh replay: %v, %v", got, err)
	}
}
