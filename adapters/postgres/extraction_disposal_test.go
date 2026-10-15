package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testExtractionDisposal(t *testing.T, ctx context.Context, normal *facts.Engine, runtimeLedger *postgres.Ledger, store *evidence.Postgres, admin *pgxpool.Pool) {
	cfg := admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["role"] = "facts_retention"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	_, private, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := audit.NewEd25519("extract-retention", private)
	engine, err := facts.NewWithAudit(ledger, allow{}, store, model.AuditOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	c := model.Caller{Tenant: "extraction-retention", Principal: "operator"}
	worker := c
	worker.Service = true
	const scope = "scope"
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "retention"} }
	registerTestIdentities(t, ctx, normal, c.Tenant, scope, []string{"subject"})
	registerTestIdentities(t, ctx, normal, c.Tenant, "other", nil)
	if _, err = normal.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: cmd("acceptance"), Policy: model.AcceptancePolicy{Scope: scope, ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = normal.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: cmd("predicate"), Definition: model.PredicateVersion{Scope: scope, Predicate: "value", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	source, err := store.Put(ctx, c.Tenant, "source", []byte("tea"))
	if err != nil {
		t.Fatal(err)
	}
	conversation, err := normal.IngestConversation(ctx, c, model.ConversationRequest{Command: cmd("conversation"), Scope: scope, SourceSystem: "test", ConversationID: "chat", Revision: "v1", Timezone: "UTC", Messages: []model.ConversationMessage{{ID: "message", Revision: "v1", Speaker: "customer", SpeakerRole: "customer", SourceClass: "customer", SourceTime: time.Now().UTC(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "source-policy", Object: source}}})
	if err != nil {
		t.Fatal(err)
	}
	profile := model.ExtractionProfile{ID: "profile", ProviderID: "fixture", RequestedModel: "fixture", PromptVersion: "v1", Prompt: []byte("extract"), PromptDigest: model.BytesDigest([]byte("extract")), Parameters: json.RawMessage(`{}`), ParserVersion: "facts-extraction-json-v1", ApplicationBuild: "test", Timeout: time.Second, MaxInputBytes: 1 << 20, MaxOutputBytes: 1 << 20, MaxCandidates: 4, MaxAttempts: 1, MaxTokensPerAttempt: 100, RetryDelay: time.Second}
	response := []byte(`{"candidates":[]}`)
	runner, err := facts.NewWithOptions(runtimeLedger, allow{}, store, model.EngineOptions{Extraction: model.ExtractionOptions{Profiles: []model.ApprovedExtractor{{Profile: profile, Provider: extractionFunc(func(context.Context, []byte) (model.ExtractionResponse, error) {
		return model.ExtractionResponse{ModelIdentity: "fixture", Body: response}, nil
	})}}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 100000}})
	if err != nil {
		t.Fatal(err)
	}
	start := func(key string, run bool) model.JobStatus {
		t.Helper()
		j, err := runner.StartExtraction(ctx, c, model.ExtractionRequest{Command: cmd(key), Scope: scope, ConversationID: conversation.ID, ProfileID: profile.ID, Subjects: []string{"subject"}, Predicates: []model.PredicateBinding{{Predicate: "value", SchemaVersion: "v1"}}})
		if err != nil {
			t.Fatal(err)
		}
		if run {
			j, err = runner.RunExtraction(ctx, worker, model.ExtractionRunRequest{Command: cmd("run-" + key), ID: j.ID, Scope: scope})
			if err != nil {
				t.Fatal(err)
			}
		}
		return j
	}
	for _, kind := range []string{"job", "request", "response"} {
		for _, young := range []bool{false, true} {
			p := model.RetentionPolicy{ID: kind, Scope: scope, RecordClass: "extraction_" + kind + "_payload", Purpose: "retention", Jurisdiction: "SG"}
			if young {
				p.ID += "-young"
				p.MinimumAgeSeconds = 3600
			}
			if _, err = engine.RegisterRetentionPolicy(ctx, c, model.RetentionPolicyRequest{Command: cmd("policy-" + p.ID), Policy: p}); err != nil {
				t.Fatal(err)
			}
		}
	}
	request := func(j model.JobStatus) model.ExtractionDisposalRequest {
		return model.ExtractionDisposalRequest{Command: cmd("dispose-" + j.ID), JobID: j.ID, Reason: "expired", Bindings: []model.ExtractionRetentionBinding{{Scope: scope, JobPolicyID: "job", RequestPolicyID: "request", ResponsePolicyID: "response", Jurisdiction: "SG"}}}
	}
	reject := func(e *facts.Engine, r model.ExtractionDisposalRequest, want error) {
		t.Helper()
		if _, err := e.DisposeExtraction(ctx, c, r); !errors.Is(err, want) {
			t.Fatalf("disposal got %v want %v", err, want)
		}
	}
	pending := start("pending", false)
	reject(engine, request(pending), model.ErrPrecondition)
	wrongScope := request(pending)
	wrongScope.Bindings[0].Scope = "other"
	reject(engine, wrongScope, model.ErrDenied)
	wrongScope.JobID = "unknown-job"
	reject(engine, wrongScope, model.ErrDenied)
	response = []byte("malformed")
	failed := start("failed", true)
	if failed.State != "failed" {
		t.Fatal(failed)
	}
	oldAttempt := failed.AttemptID
	if _, err = runner.RetryJob(ctx, c, model.JobRetryRequest{Command: cmd("retry"), ID: failed.ID, Scope: scope, ExpectedFence: failed.Fence}); err != nil {
		t.Fatal(err)
	}
	response = []byte(`{"candidates":[]}`)
	completed, err := runner.RunExtraction(ctx, worker, model.ExtractionRunRequest{Command: cmd("rerun"), ID: failed.ID, Scope: scope})
	if err != nil || completed.State != "completed" {
		t.Fatal(completed, err)
	}
	exported := start("exported", true)
	value := "tea"
	from := time.Now().UTC().Add(-time.Hour)
	response, err = json.Marshal(model.ExtractionOutput{Candidates: []model.ExtractionCandidate{{Subject: "subject", Predicate: "value", SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &from}, Sources: []model.SourceSpan{{EvidenceID: conversation.EvidenceIDs[0], Start: 0, End: 3, Quote: "tea"}}, Uncertainty: "asserted"}}})
	if err != nil {
		t.Fatal(err)
	}
	derived := start("derived", true)
	derivedRun, err := runner.GetExtractionRun(ctx, c, model.ExtractionRunQuery{ID: derived.AttemptID, Scope: scope, Purpose: "read"})
	if err != nil || derivedRun.Result == nil || len(derivedRun.Result.Proposals) != 1 {
		t.Fatal("missing derived fixture", derivedRun, err)
	}
	reject(engine, request(derived), model.ErrPrecondition)
	reject(normal, request(completed), model.ErrDenied)
	for _, kind := range []string{"job", "request", "response"} {
		r := request(completed)
		switch kind {
		case "job":
			r.Bindings[0].JobPolicyID += "-young"
		case "request":
			r.Bindings[0].RequestPolicyID += "-young"
		case "response":
			r.Bindings[0].ResponsePolicyID += "-young"
		}
		reject(engine, r, model.ErrPrecondition)
	}
	bad := request(completed)
	bad.Bindings[0].RequestPolicyID = "response"
	reject(engine, bad, model.ErrPrecondition)
	bad = request(completed)
	bad.Purpose = "other"
	reject(engine, bad, model.ErrPrecondition)
	bad = request(completed)
	bad.Bindings[0].Jurisdiction = "US"
	reject(engine, bad, model.ErrPrecondition)
	bad = request(completed)
	bad.Bindings[0].Scope = "other"
	reject(engine, bad, model.ErrDenied)
	denied, err := facts.New(ledger, denyScope{scope: scope}, store)
	if err != nil {
		t.Fatal(err)
	}
	reject(denied, request(completed), model.ErrDenied)
	if _, err = engine.PlaceLegalHold(ctx, c, model.LegalHoldRequest{Command: cmd("hold"), Scope: scope, ID: "hold", Reason: "review"}); err != nil {
		t.Fatal(err)
	}
	reject(engine, request(completed), model.ErrPrecondition)
	if _, err = engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: cmd("release"), Scope: scope, ID: "hold", Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,'archive-pending',3)", c.Tenant); err != nil {
		t.Fatal(err)
	}
	reject(engine, request(completed), model.ErrPrecondition)
	if _, err = admin.Exec(ctx, "DELETE FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id='archive-pending'", c.Tenant); err != nil {
		t.Fatal(err)
	}
	// An incomplete legacy inventory must be backfilled before any payload removal.
	if _, err = admin.Exec(ctx, "DELETE FROM extraction_attempt_inventories WHERE tenant_id=$1 AND attempt_id=$2", c.Tenant, oldAttempt); err != nil {
		t.Fatal(err)
	}
	reject(engine, request(completed), model.ErrPrecondition)
	backfill, err := runtimeLedger.BackfillExtractionInputInventories(ctx, c.Tenant, 100)
	if err != nil || backfill.Inventoried != 1 || !backfill.Complete {
		t.Fatal(backfill, err)
	}
	// A failure after the job and request deletes must roll the entire command back.
	if _, err = admin.Exec(ctx, "REVOKE DELETE ON extraction_result_payloads FROM facts_retention"); err != nil {
		t.Fatal(err)
	}
	reject(engine, request(completed), model.ErrDenied)
	if _, err = admin.Exec(ctx, "GRANT DELETE ON extraction_result_payloads TO facts_retention"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{oldAttempt, completed.AttemptID} {
		if _, err = runner.GetExtractionRun(ctx, c, model.ExtractionRunQuery{ID: id, Scope: scope, Purpose: "read"}); err != nil {
			t.Fatal("rollback lost artifact", err)
		}
	}
	multi, multiRequest := extractionDisposalCrossScopeFixture(t, ctx, normal, engine, runtimeLedger, store, c, conversation, profile)
	incomplete := multiRequest
	incomplete.Bindings = append([]model.ExtractionRetentionBinding(nil), multiRequest.Bindings[:1]...)
	reject(engine, incomplete, model.ErrDenied)
	dependencyDenied, err := facts.New(ledger, denyScope{scope: "dependency"}, store)
	if err != nil {
		t.Fatal(err)
	}
	reject(dependencyDenied, multiRequest, model.ErrDenied)
	if _, err = engine.PlaceLegalHold(ctx, c, model.LegalHoldRequest{Command: cmd("dependency-hold"), Scope: "dependency", ID: "hold", Reason: "review"}); err != nil {
		t.Fatal(err)
	}
	reject(engine, multiRequest, model.ErrPrecondition)
	if _, err = engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: cmd("dependency-release"), Scope: "dependency", ID: "hold", Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	var jobRevision int64
	if err = admin.QueryRow(ctx, "SELECT ledger_revision FROM extraction_jobs WHERE tenant_id=$1 AND id=$2", c.Tenant, completed.ID).Scan(&jobRevision); err != nil {
		t.Fatal(err)
	}
	verifyArchiveReplay := archiveRevisionsBeforeDisposal(t, ctx, admin, cipher, signer, map[string]ed25519.PublicKey{"extract-retention": private.Public().(ed25519.PublicKey)}, c.Tenant, jobRevision, multi.CreatedRevision)
	first, err := engine.DisposeExtraction(ctx, c, request(completed))
	if err != nil {
		t.Fatal(err)
	}
	testExtractionDisposalRemote(t, ctx, engine, c, request(completed), first)
	multiReceipt, err := engine.DisposeExtraction(ctx, c, multiRequest)
	if err != nil {
		t.Fatal(err)
	}
	multiRequest.Bindings[0], multiRequest.Bindings[1] = multiRequest.Bindings[1], multiRequest.Bindings[0]
	testExtractionDisposalRemote(t, ctx, engine, c, multiRequest, multiReceipt)
	if got, err := normal.GetFact(ctx, c, model.FactQuery{Key: model.FactKey{Scope: "dependency", Subject: "subject", Predicate: "value"}, ValidAt: time.Now().UTC(), Purpose: "read"}); err != nil || got.Value == nil || got.Value.Text == nil || *got.Value.Text != "tea" {
		t.Fatal("source fact lost", got, err)
	}
	verifyArchiveReplay()
	reject(denied, request(completed), model.ErrDenied)
	fresh := request(completed)
	fresh.Key = "new-disposal"
	reject(engine, fresh, model.ErrPrecondition)
	for _, id := range []string{oldAttempt, completed.AttemptID} {
		if _, err = runner.GetExtractionRun(ctx, c, model.ExtractionRunQuery{ID: id, Scope: scope, Purpose: "read"}); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("disposed request returned", err)
		}
	}
	if _, err = runner.RunExtraction(ctx, worker, model.ExtractionRunRequest{Command: cmd("after-disposal"), ID: completed.ID, Scope: scope}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("disposed job executed", err)
	}
	var count int
	for _, query := range []string{
		"SELECT count(*) FROM extraction_job_payloads WHERE tenant_id=$1 AND job_id=$2",
		"SELECT count(*) FROM extraction_attempt_payloads p JOIN extraction_attempts a ON a.tenant_id=p.tenant_id AND a.id=p.attempt_id WHERE a.tenant_id=$1 AND a.job_id=$2",
		"SELECT count(*) FROM extraction_result_payloads p JOIN extraction_attempts a ON a.tenant_id=p.tenant_id AND a.id=p.attempt_id WHERE a.tenant_id=$1 AND a.job_id=$2",
	} {
		if err = admin.QueryRow(ctx, query, c.Tenant, completed.ID).Scan(&count); err != nil || count != 0 {
			t.Fatal("retained payload", count, err)
		}
	}
	for _, query := range []string{
		"SELECT count(*) FROM extraction_attempt_disposals d JOIN extraction_attempts a ON a.tenant_id=d.tenant_id AND a.id=d.attempt_id WHERE a.tenant_id=$1 AND a.job_id=$2",
		"SELECT count(*) FROM extraction_result_disposals d JOIN extraction_attempts a ON a.tenant_id=d.tenant_id AND a.id=d.attempt_id WHERE a.tenant_id=$1 AND a.job_id=$2",
	} {
		if err = admin.QueryRow(ctx, query, c.Tenant, completed.ID).Scan(&count); err != nil || count != 2 {
			t.Fatal("incomplete retry history disposal", count, err)
		}
	}
	export, err := engine.StartAuditExport(ctx, c, model.AuditExportRequest{Command: cmd("export"), Scope: scope, IncludeEvidence: true})
	if err != nil {
		t.Fatal(err)
	}
	reject(engine, request(exported), model.ErrPrecondition)
	bundle, err := engine.RunAuditExport(ctx, c, model.AuditExportQuery{ID: export.ID, Scope: scope, Purpose: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	gaps := map[string]int{}
	for _, gap := range bundle.Bundle.Manifest.Gaps {
		if gap.Reason == "disposed_under_retention_policy" {
			gaps[gap.Kind]++
		}
	}
	if gaps["extraction_job_content"] != 1 || gaps["extraction_attempt_content"] != 2 || gaps["extraction_result_content"] != 2 {
		t.Fatal("missing disposal gaps", gaps)
	}
	if err = audit.Verify(*bundle.Bundle, map[string]ed25519.PublicKey{"extract-retention": private.Public().(ed25519.PublicKey)}); err != nil {
		t.Fatal(err)
	}
}

// A real copied dependency in a second scope must participate in authorization,
// legal holds and policy selection even though the job belongs to the first scope.
func extractionDisposalCrossScopeFixture(t *testing.T, ctx context.Context, normal, retention *facts.Engine, l *postgres.Ledger, store *evidence.Postgres, c model.Caller, conversation model.ConversationRecord, profile model.ExtractionProfile) (model.JobStatus, model.ExtractionDisposalRequest) {
	t.Helper()
	const scope = "dependency"
	cmd := func(key string) model.Command { return model.Command{Key: "cross-" + key, Purpose: "retention"} }
	registerTestIdentities(t, ctx, normal, c.Tenant, scope, []string{"subject"})
	if _, err := normal.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{ID: "manual", Scope: scope, Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := normal.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: cmd("predicate"), Definition: model.PredicateVersion{Scope: scope, Predicate: "value", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	object, err := store.Put(ctx, c.Tenant, "cross-source", []byte("tea"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Hour)
	if _, err = normal.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: cmd("evidence"), Evidence: model.Evidence{ID: "cross-source", Scope: scope, SourceSystem: "test", SourceObject: "source", SourceRevision: "v1", Speaker: "customer", SourceClass: "customer", SourceTime: now, MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "source-policy", Object: object}}); err != nil {
		t.Fatal(err)
	}
	value := "tea"
	fact, err := normal.ProposeFact(ctx, c, model.FactProposal{Command: cmd("fact"), Key: model.FactKey{Scope: scope, Subject: "subject", Predicate: "value"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &now}, Sources: []model.SourceSpan{{EvidenceID: "cross-source", Start: 0, End: 3, Quote: value}}})
	if err != nil {
		t.Fatal(err)
	}
	reviewer := c
	reviewer.Principal = "reviewer"
	if _, err = normal.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("approve"), Scope: scope, RecordID: fact.RecordID, VersionID: fact.VersionID, ExpectedRevision: fact.StreamRevision}); err != nil {
		t.Fatal(err)
	}
	if _, err = normal.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: cmd("type"), Definition: model.MemoryType{Scope: conversation.Request.Scope, Subtype: "summary", Version: "v1", PolicyID: "manual", Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"job", "request", "response"} {
		if _, err = retention.RegisterRetentionPolicy(ctx, c, model.RetentionPolicyRequest{Command: cmd(kind), Policy: model.RetentionPolicy{ID: kind, Scope: scope, RecordClass: "extraction_" + kind + "_payload", Purpose: "retention", Jurisdiction: "SG"}}); err != nil {
			t.Fatal(err)
		}
	}
	profile.ID = "memory-profile"
	profile.ParserVersion = "memory-extraction-json-v1"
	runner, err := facts.NewWithOptions(l, allow{}, store, model.EngineOptions{Extraction: model.ExtractionOptions{Profiles: []model.ApprovedExtractor{{Profile: profile, Provider: extractionFunc(func(context.Context, []byte) (model.ExtractionResponse, error) {
		return model.ExtractionResponse{ModelIdentity: "fixture", Body: []byte(`{"candidates":[]}`)}, nil
	})}}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 100000}})
	if err != nil {
		t.Fatal(err)
	}
	job, err := runner.StartExtraction(ctx, c, model.ExtractionRequest{Command: cmd("start"), Scope: conversation.Request.Scope, ConversationID: conversation.ID, ProfileID: profile.ID, MemoryTargets: []model.MemoryExtractionTarget{{ID: "summary", Subtype: "summary", SchemaVersion: "v1", Dependencies: []model.MemoryDependency{{Scope: scope, RecordID: fact.RecordID, VersionID: fact.VersionID, ContentDigest: fact.ContentDigest, Label: "assertion"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	worker := c
	worker.Service = true
	job, err = runner.RunExtraction(ctx, worker, model.ExtractionRunRequest{Command: cmd("run"), ID: job.ID, Scope: job.Scope})
	if err != nil || job.State != "completed" {
		t.Fatal(job, err)
	}
	request := model.ExtractionDisposalRequest{Command: cmd("dispose"), JobID: job.ID, Reason: "expired"}
	for _, s := range []string{conversation.Request.Scope, scope} {
		request.Bindings = append(request.Bindings, model.ExtractionRetentionBinding{Scope: s, JobPolicyID: "job", RequestPolicyID: "request", ResponsePolicyID: "response", Jurisdiction: "SG"})
	}
	return job, request
}
