package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
)

func testMemoryRefresh(t *testing.T, ctx context.Context, l model.Ledger, store *evidence.Postgres, queue model.Outbox, c model.Caller) {
	t.Helper()
	base, err := facts.New(l, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	scope := "refresh-case"
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "refresh-review"} }
	reviewer := model.Caller{Tenant: c.Tenant, Principal: "refresh-reviewer"}
	worker := model.Caller{Tenant: c.Tenant, Principal: "refresh-worker", Service: true}
	if _, err = base.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: cmd("refresh-policy"), Policy: model.AcceptancePolicy{Scope: scope, ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = base.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: cmd("refresh-predicate"), Definition: model.PredicateVersion{Scope: scope, Predicate: "preference", Version: "v1", Kind: model.Text, PolicyID: "manual", Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = base.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: cmd("refresh-type"), Definition: model.MemoryType{Scope: scope, Subtype: "summary", Version: "v1", PolicyID: "manual", Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	source := "I prefer tea. Now I prefer coffee."
	object, err := store.Put(ctx, c.Tenant, "refresh-evidence", []byte(source))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = base.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: cmd("refresh-evidence"), Evidence: model.Evidence{ID: "refresh-evidence", Scope: scope, SourceSystem: "customer", SourceObject: "refresh", SourceRevision: "v1", Speaker: "customer", SourceClass: "customer", SourceTime: time.Now().UTC(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "retain", Object: object}}); err != nil {
		t.Fatal(err)
	}
	from := time.Now().UTC().Truncate(time.Microsecond)
	value := "tea"
	proposal := model.FactProposal{Command: cmd("refresh-fact"), Key: model.FactKey{Scope: scope, Subject: "customer", Predicate: "preference"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &from}, Sources: []model.SourceSpan{{EvidenceID: "refresh-evidence", Start: 9, End: 12, Quote: "tea"}}}
	fact, err := base.ProposeFact(ctx, c, proposal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = base.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("refresh-fact-approve"), Scope: scope, RecordID: fact.RecordID, VersionID: fact.VersionID, ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	memoryRequest := model.MemoryProposal{Command: cmd("refresh-memory"), Key: model.MemoryKey{Scope: scope, ID: "summary"}, Subtype: "summary", SchemaVersion: "v1", Body: "Customer prefers tea.", Valid: proposal.Valid, Sources: proposal.Sources, Dependencies: []model.MemoryDependency{{Scope: scope, RecordID: fact.RecordID, VersionID: fact.VersionID, ContentDigest: fact.ContentDigest, Label: "assertion"}}}
	original, err := base.ProposeMemory(ctx, c, memoryRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = base.ApproveMemory(ctx, reviewer, model.Approval{Command: cmd("refresh-memory-approve"), Scope: scope, RecordID: original.RecordID, VersionID: original.VersionID, ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	prompt := []byte("Propose a replacement summary using only supplied dependencies. Never accept a version.")
	profile := model.ExtractionProfile{ID: "refresh-profile", ProviderID: "fixture", RequestedModel: "fixture-v1", PromptVersion: "v1", Prompt: prompt, PromptDigest: model.BytesDigest(prompt), ParserVersion: "memory-refresh-json-v1", Parameters: json.RawMessage(`{}`), ApplicationBuild: "test-v1", Timeout: time.Second, MaxInputBytes: 1 << 20, MaxOutputBytes: 1 << 20, MaxCandidates: 1, MaxAttempts: 2, MaxTokensPerAttempt: 1000, RetryDelay: time.Second}
	var engine *facts.Engine
	var calls int
	provider := extractionFunc(func(ctx context.Context, raw []byte) (model.ExtractionResponse, error) {
		calls++
		var input model.ExtractionInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return model.ExtractionResponse{}, err
		}
		if input.Refresh == nil || input.Refresh.Original.ID != original.VersionID || len(input.Refresh.Dependencies) != 1 || len(input.Refresh.Dependencies[0].Versions) != 1 {
			return model.ExtractionResponse{}, errors.New("missing refresh provenance")
		}
		retained, err := engine.GetExtractionRun(ctx, worker, model.ExtractionRunQuery{ID: input.AttemptID, Scope: scope, Purpose: "refresh-review"})
		if err != nil || !bytes.Equal(retained.Attempt.RequestBytes, raw) {
			return model.ExtractionResponse{}, errors.New("request not durably retained")
		}
		current := input.Refresh.Dependencies[0].Versions[0]
		body, _ := json.Marshal(model.MemoryRefreshOutput{Body: "Customer now prefers coffee.", Dependencies: []model.MemoryDependency{{Scope: scope, RecordID: current.RecordID, VersionID: current.ID, ContentDigest: current.Digest, Label: "assertion"}}})
		return model.ExtractionResponse{ModelIdentity: "fixture-v1", Body: body, InputTokens: 10, OutputTokens: 10}, nil
	})
	engine, err = facts.NewWithOptions(l, allow{}, store, model.EngineOptions{Extraction: model.ExtractionOptions{Profiles: []model.ApprovedExtractor{{Profile: profile, Provider: provider}}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 100000}})
	if err != nil {
		t.Fatal(err)
	}
	request := model.MemoryRefreshRequest{Command: cmd("refresh-start"), Scope: scope, RecordID: original.RecordID, VersionID: original.VersionID, ProfileID: profile.ID}
	if _, err = engine.StartMemoryRefresh(ctx, c, request); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("eligible memory scheduled for refresh", err)
	}
	value = "coffee"
	proposal.Value.Text = &value
	proposal.Command = cmd("refresh-fact-correction")
	proposal.ExpectedRevision = 2
	start := strings.Index(source, "coffee")
	proposal.Sources = []model.SourceSpan{{EvidenceID: "refresh-evidence", Start: start, End: start + 6, Quote: "coffee"}}
	corrected, err := base.ProposeFact(ctx, c, proposal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = base.SupersedeFact(ctx, reviewer, model.Supersession{Approval: model.Approval{Command: cmd("refresh-fact-supersede"), Scope: scope, RecordID: fact.RecordID, VersionID: corrected.VersionID, ExpectedRevision: 3}, Valid: proposal.Valid, ReplacedVersionIDs: []string{fact.VersionID}, Reason: "correction"}); err != nil {
		t.Fatal(err)
	}
	tinyProfile := profile
	tinyProfile.ID = "refresh-tiny"
	tinyProfile.MaxInputBytes = 1
	tiny, err := facts.NewWithOptions(l, allow{}, store, model.EngineOptions{Extraction: model.ExtractionOptions{Profiles: []model.ApprovedExtractor{{Profile: tinyProfile, Provider: provider}}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 100000}})
	if err != nil {
		t.Fatal(err)
	}
	tinyRequest := request
	tinyRequest.Command = cmd("refresh-tiny-start")
	tinyRequest.ProfileID = tinyProfile.ID
	tinyJob, err := tiny.StartMemoryRefresh(ctx, c, tinyRequest)
	if err != nil {
		t.Fatal(err)
	}
	tinyResult, err := tiny.RunExtraction(ctx, worker, model.ExtractionRunRequest{Command: cmd("refresh-tiny-run"), ID: tinyJob.ID, Scope: scope})
	if err != nil || tinyResult.State != "failed" || tinyResult.LastFailureCode != "input_limit" || calls != 0 {
		t.Fatal("oversized refresh did not fail before provider", tinyResult, err)
	}
	job := testAutomaticMemoryRefresh(t, ctx, engine, l, store, queue, worker, request, original)
	if calls != 0 {
		t.Fatal("scheduler invoked model")
	}
	handler, err := workers.NewExtraction(engine, worker, "refresh-test")
	if err != nil {
		t.Fatal(err)
	}
	delivery := model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant, Consumer: model.ExtractionConsumer("refresh-test")}, Fence: 1, Event: model.Event{ID: "refresh-event", EntityID: job.ID, Scope: scope, Type: "start_memory_refresh", Caller: c, Command: request.Command}}
	if err = handler.Handle(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	completed, err := engine.GetJob(ctx, c, model.JobQuery{ID: job.ID, Scope: scope, Purpose: "refresh-review"})
	if err != nil || completed.State != "completed" {
		t.Fatal(completed, err)
	}
	run, err := engine.GetExtractionRun(ctx, c, model.ExtractionRunQuery{ID: completed.AttemptID, Scope: scope, Purpose: "refresh-review"})
	if err != nil || run.Result == nil || len(run.Result.Proposals) != 1 || run.Result.Proposals[0].RecordKind != "memory" {
		t.Fatal(run, err)
	}
	denied, err := facts.New(l, denyPermission{permission: "can_read_history"}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = denied.GetExtractionRun(ctx, c, model.ExtractionRunQuery{ID: completed.AttemptID, Scope: scope, Purpose: "refresh-review"}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("refresh artifact history permission bypassed", err)
	}
	refreshed := run.Result.Proposals[0]
	version, err := engine.GetMemoryVersion(ctx, c, model.MemoryVersionQuery{Scope: scope, RecordID: refreshed.RecordID, VersionID: refreshed.VersionID, Purpose: "refresh-review"})
	if err != nil || version.Proposal.Dependencies[0].VersionID != corrected.VersionID {
		t.Fatal(version, err)
	}
	current, err := engine.GetMemory(ctx, c, model.MemoryQuery{Key: memoryRequest.Key, ValidAt: from, Purpose: "refresh-review"})
	if err != nil || current.State != "stale" || current.Body != "" {
		t.Fatal("refresh automatically replaced memory", current, err)
	}
	if _, err = engine.RunExtraction(ctx, worker, model.ExtractionRunRequest{Command: cmd("refresh-run-again"), ID: job.ID, Scope: scope}); err != nil || calls != 1 {
		t.Fatal("refresh replay invoked provider", calls, err)
	}
	if _, err = engine.SupersedeMemory(ctx, reviewer, model.Supersession{Approval: model.Approval{Command: cmd("refresh-memory-supersede"), Scope: scope, RecordID: original.RecordID, VersionID: refreshed.VersionID, ExpectedRevision: 3}, Valid: memoryRequest.Valid, ReplacedVersionIDs: []string{original.VersionID}, Reason: "dependency_correction"}); err != nil {
		t.Fatal(err)
	}
	current, err = engine.GetMemory(ctx, c, model.MemoryQuery{Key: memoryRequest.Key, ValidAt: from, Purpose: "refresh-review"})
	if err != nil || current.State != "resolved" || current.Body != "Customer now prefers coffee." {
		t.Fatal(current, err)
	}
	old, err := engine.GetMemoryVersion(ctx, c, model.MemoryVersionQuery{Scope: scope, RecordID: original.RecordID, VersionID: original.VersionID, Purpose: "refresh-review"})
	if err != nil || old.Proposal.Body != "Customer prefers tea." {
		t.Fatal("refresh rewrote original", old, err)
	}
	testExpiryMemoryRefresh(t, ctx, l, store, queue, c, reviewer, worker, profile, proposal)

}
