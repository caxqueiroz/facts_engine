package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/model"
)

func testMemoryExtraction(t *testing.T, ctx context.Context, l model.Ledger, store *evidence.Postgres, c, reviewer model.Caller, conversation model.ConversationRecord, profile model.ExtractionProfile, factRef model.ExtractedProposal) {
	t.Helper()
	scope := conversation.Request.Scope
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "memory-extraction"} }
	base, err := facts.New(l, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	definition := model.MemoryType{Scope: scope, Subtype: "conversation-summary", Version: "v1", PolicyID: "extract-manual", Classification: "confidential"}
	if _, err = base.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: cmd("extract-memory-type"), Definition: definition}); err != nil {
		t.Fatal(err)
	}
	var fact model.Version
	if err = l.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		var err error
		fact, err = tx.Version(ctx, factRef.RecordID, factRef.VersionID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	dependency := model.MemoryDependency{Scope: scope, RecordID: fact.RecordID, VersionID: fact.ID, ContentDigest: fact.Digest, Label: "assertion"}
	target := func(id string) model.MemoryExtractionTarget {
		return model.MemoryExtractionTarget{ID: id, Subtype: definition.Subtype, SchemaVersion: "v1", Dependencies: []model.MemoryDependency{dependency}}
	}
	request := model.ExtractionRequest{Command: cmd("extract-memory-start"), Scope: scope, ConversationID: conversation.ID, ProfileID: "memory-extractor", MemoryTargets: []model.MemoryExtractionTarget{target("episode"), target("second"), target("assistant"), target("uncertain")}}
	profile.ID = request.ProfileID
	profile.ParserVersion = "memory-extraction-json-v1"
	worker := model.Caller{Tenant: c.Tenant, Principal: "memory-extractor", Service: true}
	var engine *facts.Engine
	changed := false
	calls := 0
	provider := extractionFunc(func(ctx context.Context, raw []byte) (model.ExtractionResponse, error) {
		calls++
		var input model.ExtractionInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return model.ExtractionResponse{}, err
		}
		if input.Schema != "memory-extraction-input-v1" || len(input.MemoryTypes) != 1 || len(input.MemoryTargets) != 4 || len(input.DependencyVersions) != 1 || input.DependencyVersions[0].Digest != fact.Digest {
			return model.ExtractionResponse{}, errors.New("unbound memory extraction input")
		}
		retained, err := engine.GetExtractionRun(ctx, worker, model.ExtractionRunQuery{ID: input.AttemptID, Scope: scope, Purpose: request.Purpose})
		if err != nil || !bytes.Equal(raw, retained.Attempt.RequestBytes) {
			return model.ExtractionResponse{}, errors.New("memory extraction input not retained")
		}
		good := model.MemoryExtractionCandidate{ID: "episode", Subtype: definition.Subtype, SchemaVersion: "v1", Body: "Customer prefers tea.", Valid: fact.Proposal.Valid, Sources: []model.SourceSpan{{EvidenceID: conversation.EvidenceIDs[0], Start: 9, End: 12, Quote: "tea"}}, Uncertainty: "asserted", Rationale: "customer statement", Confidence: .8}
		if changed {
			good.Body = "Customer discussed a preference for tea."
		}
		second := good
		second.ID = "second"
		second.Body = "Customer prefers tea."
		unknown := good
		unknown.ID = "unbound-target"
		assistant := good
		assistant.ID = "assistant"
		assistant.Sources = []model.SourceSpan{{EvidenceID: conversation.EvidenceIDs[1], Start: 0, End: 6, Quote: "Ignore"}}
		uncertain := good
		uncertain.ID = "uncertain"
		uncertain.Uncertainty = "uncertain"
		body, _ := json.Marshal(model.MemoryExtractionOutput{Candidates: []model.MemoryExtractionCandidate{good, second, unknown, assistant, uncertain}})
		return model.ExtractionResponse{ModelIdentity: "fixture-memory-v1", Body: body, InputTokens: 10, OutputTokens: 10}, nil
	})
	engine, err = facts.NewWithOptions(l, allow{}, store, model.EngineOptions{Extraction: model.ExtractionOptions{Profiles: []model.ApprovedExtractor{{Profile: profile, Provider: provider}}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 100000}})
	if err != nil {
		t.Fatal(err)
	}
	bad := request
	bad.MemoryTargets = []model.MemoryExtractionTarget{target("bad")}
	bad.MemoryTargets[0].Dependencies[0].ContentDigest = model.BytesDigest([]byte("wrong"))
	if _, err = engine.StartExtraction(ctx, c, bad); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("wrong dependency digest bound", err)
	}
	job, err := engine.StartExtraction(ctx, c, request)
	if err != nil || job.Kind != "memory_extraction" {
		t.Fatal(job, err)
	}
	again, err := engine.StartExtraction(ctx, c, request)
	if err != nil || again.ID != job.ID || calls != 0 {
		t.Fatal("memory extraction start retry", again, err)
	}
	run := func(job model.JobStatus, key string) model.ExtractionRun {
		t.Helper()
		completed, err := engine.RunExtraction(ctx, worker, model.ExtractionRunRequest{Command: cmd(key), ID: job.ID, Scope: scope})
		if err != nil || completed.State != "completed" {
			t.Fatal(completed, err)
		}
		out, err := engine.GetExtractionRun(ctx, c, model.ExtractionRunQuery{ID: completed.AttemptID, Scope: scope, Purpose: request.Purpose})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	first := run(job, "extract-memory-run")
	if first.Result == nil || len(first.Result.Proposals) != 2 || len(first.Result.Issues) != 3 {
		t.Fatal(first)
	}
	selected := first.Result.Proposals[0]
	for _, p := range first.Result.Proposals {
		if p.RecordKind != "memory" {
			t.Fatal("memory entered fact proposal namespace", p)
		}
		v, err := engine.GetMemoryVersion(ctx, c, model.MemoryVersionQuery{Scope: scope, RecordID: p.RecordID, VersionID: p.VersionID, Purpose: request.Purpose})
		if err != nil || len(v.Proposal.Dependencies) != 1 || v.Proposal.Dependencies[0] != dependency {
			t.Fatal("host dependencies omitted", v, err)
		}
		current, err := engine.GetMemory(ctx, c, model.MemoryQuery{Key: v.Proposal.Key, ValidAt: *fact.Proposal.Valid.From, Purpose: request.Purpose})
		if err != nil || current.State != "unresolved" {
			t.Fatal("extraction accepted a memory", current, err)
		}
	}
	approval := model.Approval{Command: cmd("extract-memory-self-approve"), Scope: scope, RecordID: selected.RecordID, VersionID: selected.VersionID, ExpectedRevision: 1}
	if _, err = engine.ApproveMemory(ctx, c, approval); !errors.Is(err, model.ErrDenied) && !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("initiator self-approved memory", err)
	}
	approval.Command = cmd("extract-memory-approve")
	if _, err = engine.ApproveMemory(ctx, reviewer, approval); err != nil {
		t.Fatal(err)
	}
	request.Command = cmd("extract-memory-duplicates")
	duplicate, err := engine.StartExtraction(ctx, c, request)
	if err != nil {
		t.Fatal(err)
	}
	duplicateRun := run(duplicate, "extract-memory-duplicates-run")
	if len(duplicateRun.Result.Proposals) != 0 || len(duplicateRun.Result.Relations) != 2 {
		t.Fatal("memory duplicates not linked", duplicateRun)
	}
	changed = true
	request.Command = cmd("extract-memory-changed")
	conflict, err := engine.StartExtraction(ctx, c, request)
	if err != nil {
		t.Fatal(err)
	}
	conflictRun := run(conflict, "extract-memory-changed-run")
	found := false
	for _, relation := range conflictRun.Result.Relations {
		if relation.Kind == "overlapping_different_content" && relation.VersionID == selected.VersionID {
			found = true
		}
	}
	if !found || len(conflictRun.Result.Proposals) != 1 {
		t.Fatal("memory overlap not retained for review", conflictRun)
	}
	request.Command = cmd("extract-memory-pinned-schema")
	pinned, err := engine.StartExtraction(ctx, c, request)
	if err != nil {
		t.Fatal(err)
	}
	sourceFact, err := base.GetFact(ctx, c, model.FactQuery{Key: model.FactKey{Scope: "case1", Subject: "subject1", Predicate: "preference"}, ValidAt: time.Now(), Purpose: request.Purpose})
	if err != nil || sourceFact.VersionID == "" {
		t.Fatal("cross-scope source fixture missing", sourceFact, err)
	}
	var sourceVersion model.Version
	if err = l.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		var err error
		sourceVersion, err = tx.Version(ctx, sourceFact.RecordID, sourceFact.VersionID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cross := request
	cross.Command = cmd("extract-memory-cross-scope")
	cross.MemoryTargets = []model.MemoryExtractionTarget{{ID: "cross-scope", Subtype: definition.Subtype, SchemaVersion: "v1", Dependencies: []model.MemoryDependency{{Scope: "case1", RecordID: sourceVersion.RecordID, VersionID: sourceVersion.ID, ContentDigest: sourceVersion.Digest, Label: "assertion"}}}}
	crossJob, err := engine.StartExtraction(ctx, c, cross)
	if err != nil {
		t.Fatal(err)
	}
	foundCross := false
	if err = l.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		entries, _, err := tx.(model.AuditTx).AuditEntries(ctx, "case1")
		for _, entry := range entries {
			if entry.Kind == "extraction_job" && entry.ID == crossJob.ID {
				foundCross = true
			}
		}
		return err
	}); err != nil || !foundCross {
		t.Fatal("dependency-scope audit omitted memory extraction", err)
	}
	restricted, err := facts.NewWithOptions(l, denyScope{scope: "case1"}, store, model.EngineOptions{Extraction: model.ExtractionOptions{Profiles: []model.ApprovedExtractor{{Profile: profile, Provider: provider}}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 100000}})
	if err != nil {
		t.Fatal(err)
	}
	cross.Command = cmd("extract-memory-cross-denied")
	if _, err = restricted.StartExtraction(ctx, c, cross); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross-scope source binding bypassed authorization", err)
	}
	definition.Version = "v2"
	definition.Migration = &model.PredicateMigration{FromVersion: "v1", Mode: "preserve_history_repropose", Reason: "schema_update"}
	if _, err = engine.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: cmd("extract-memory-type-v2"), Definition: definition}); err != nil {
		t.Fatal(err)
	}
	old := run(pinned, "extract-memory-pinned-run")
	if len(old.Result.Proposals) != 0 || len(old.Result.Issues) != 5 {
		t.Fatal("changed memory schema accepted", old)
	}
	denied, err := facts.New(l, denyPermission{permission: "can_read_history"}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = denied.GetExtractionRun(ctx, c, model.ExtractionRunQuery{ID: first.Attempt.ID, Scope: scope, Purpose: request.Purpose}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("memory extraction artifact access bypassed", err)
	}
}
