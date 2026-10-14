package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testExtractionCost(t *testing.T, ctx context.Context, l *postgres.Ledger, store *evidence.Postgres, admin *pgxpool.Pool) {
	base, _ := facts.New(l, allow{}, store)
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "cost-test"} }
	prompt := []byte("Return no candidates.")
	profile := model.ExtractionProfile{ID: "priced", ProviderID: "fixture", RequestedModel: "fixture-v1", PromptVersion: "v1", Prompt: prompt, PromptDigest: model.BytesDigest(prompt), ParserVersion: "facts-extraction-json-v1", ApplicationBuild: "test", Parameters: json.RawMessage(`{}`), Timeout: time.Second, MaxInputBytes: 1 << 20, MaxOutputBytes: 1 << 20, MaxCandidates: 4, MaxAttempts: 2, MaxTokensPerAttempt: 100, RetryDelay: time.Second, CostUnit: "test-microunits", MaxCostUnitsPerAttempt: 6}
	var calls atomic.Int32
	provider := extractionFunc(func(context.Context, []byte) (model.ExtractionResponse, error) {
		if calls.Add(1) == 1 {
			return model.ExtractionResponse{}, errors.New("ambiguous provider failure")
		}
		return model.ExtractionResponse{ModelIdentity: "fixture", Body: []byte(`{"candidates":[]}`)}, nil
	})
	configured := func(p model.ExtractionProfile, budget int64) *facts.Engine {
		t.Helper()
		e, err := facts.NewWithOptions(l, allow{}, store, model.EngineOptions{Extraction: model.ExtractionOptions{Profiles: []model.ApprovedExtractor{{Profile: p, Provider: provider}}, MaxConcurrentPerTenant: 2, DailyTokenBudget: 10000, CostUnit: p.CostUnit, DailyCostBudgetUnits: budget}})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	setup := func(tenant string) (model.Caller, string) {
		t.Helper()
		c := model.Caller{Tenant: tenant, Principal: "requester"}
		registerTestIdentities(t, ctx, base, tenant, "case", []string{"customer"})
		if _, err := base.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{Scope: "case", ID: "policy", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := base.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: cmd("schema"), Definition: model.PredicateVersion{Scope: "case", Predicate: "choice", Version: "v1", PolicyID: "policy", Kind: model.Text, Classification: "confidential"}}); err != nil {
			t.Fatal(err)
		}
		ref, err := store.Put(ctx, tenant, "message", []byte("Tea."))
		if err != nil {
			t.Fatal(err)
		}
		conversation, err := base.IngestConversation(ctx, c, model.ConversationRequest{Command: cmd("conversation"), Scope: "case", SourceSystem: "chat", ConversationID: "chat", Revision: "v1", Timezone: "UTC", Messages: []model.ConversationMessage{{ID: "message", Revision: "v1", Speaker: "customer", SpeakerRole: "customer", SourceClass: "customer", SourceTime: time.Now(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "test", Object: ref}}})
		if err != nil {
			t.Fatal(err)
		}
		return c, conversation.ID
	}
	start := func(e *facts.Engine, c model.Caller, conversation, key, profileID string) model.JobStatus {
		t.Helper()
		j, err := e.StartExtraction(ctx, c, model.ExtractionRequest{Command: cmd(key), Scope: "case", ConversationID: conversation, ProfileID: profileID, Subjects: []string{"customer"}, Predicates: []model.PredicateBinding{{Predicate: "choice", SchemaVersion: "v1"}}})
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	run := func(e *facts.Engine, c model.Caller, j model.JobStatus, key string) (model.JobStatus, error) {
		return e.RunExtraction(ctx, model.Caller{Tenant: c.Tenant, Principal: "worker", Service: true}, model.ExtractionRunRequest{Command: cmd(key), ID: j.ID, Scope: "case"})
	}
	usage := func(tenant string, wantTokens, wantCost int64) {
		t.Helper()
		var tokens, cost int64
		if err := admin.QueryRow(ctx, `SELECT reserved_tokens,reserved_cost_units FROM extraction_quota WHERE tenant_id=$1`, tenant).Scan(&tokens, &cost); err != nil || tokens != wantTokens || cost != wantCost {
			t.Fatal("reservation accounting", tokens, cost, err)
		}
	}
	a, conversation := setup("cost-a")
	e := configured(profile, 10)
	job := start(e, a, conversation, "first", profile.ID)
	pending, err := run(e, a, job, "first-run")
	if err != nil || pending.State != "pending" || calls.Load() != 1 {
		t.Fatal(pending, err, calls.Load())
	}
	usage(a.Tenant, 100, 6)
	retained, err := e.GetExtractionRun(ctx, a, model.ExtractionRunQuery{ID: pending.AttemptID, Scope: "case", Purpose: "cost-test"})
	if err != nil || retained.Attempt.Reservation == nil || retained.Attempt.Reservation.CostUnits != 6 || retained.Attempt.Reservation.DailyCostLimit != 10 || retained.Attempt.Reservation.Day == "" {
		t.Fatal("retained cost provenance", retained, err)
	}
	if _, err = admin.Exec(ctx, `UPDATE extraction_job_state SET available_at=clock_timestamp() WHERE tenant_id=$1 AND id=$2`, a.Tenant, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = run(e, a, job, "blocked-cost"); !errors.Is(err, model.ErrQuota) || calls.Load() != 1 {
		t.Fatal("failed call refunded or cost limit bypassed", err, calls.Load())
	}
	usage(a.Tenant, 100, 6)
	increased := configured(profile, 12)
	completed, err := run(increased, a, job, "retry-cost")
	if err != nil || completed.State != "completed" || calls.Load() != 2 {
		t.Fatal(completed, err)
	}
	usage(a.Tenant, 200, 12)
	if _, err = run(increased, a, job, "retry-cost"); err != nil || calls.Load() != 2 {
		t.Fatal("completed replay billed again", err)
	}
	usage(a.Tenant, 200, 12)
	queued := start(increased, a, conversation, "second", profile.ID)
	if _, err = run(increased, a, queued, "second-run"); !errors.Is(err, model.ErrQuota) {
		t.Fatal("daily cost boundary", err)
	}
	status, err := e.GetJob(ctx, a, model.JobQuery{ID: queued.ID, Scope: "case", Purpose: "cost-test"})
	if err != nil || status.Attempts != 0 {
		t.Fatal("quota rejection consumed attempt", status, err)
	}
	unpriced := profile
	unpriced.ID = "unpriced"
	unpriced.CostUnit = ""
	unpriced.MaxCostUnitsPerAttempt = 0
	disabled := configured(unpriced, 0)
	unpricedJob := start(disabled, a, conversation, "disabled", unpriced.ID)
	if _, err = run(disabled, a, unpricedJob, "disabled-run"); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("unpriced worker bypassed enabled cost meter", err)
	}
	changed := profile
	changed.ID = "wrong-unit"
	changed.CostUnit = "different-unit"
	wrong := configured(changed, 12)
	wrongJob := start(wrong, a, conversation, "wrong-unit", changed.ID)
	if _, err = run(wrong, a, wrongJob, "wrong-unit-run"); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("mixed cost units", err)
	}
	b, otherConversation := setup("cost-b")
	other := start(increased, b, otherConversation, "independent", profile.ID)
	if _, err = run(increased, b, other, "independent-run"); err != nil {
		t.Fatal("tenant quota leaked", err)
	}
	usage(b.Tenant, 100, 6)
	usage(a.Tenant, 200, 12)
	// Simulate a previous UTC accounting day without changing wall clocks.
	if _, err = admin.Exec(ctx, `UPDATE extraction_quota SET day=(clock_timestamp() AT TIME ZONE 'UTC')::date-1 WHERE tenant_id=$1`, a.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = run(increased, a, queued, "second-run"); err != nil {
		t.Fatal("UTC rollover", err)
	}
	usage(a.Tenant, 100, 6)
	racer, raceConversation := setup("cost-race")
	raceEngine := configured(profile, 6)
	jobs := []model.JobStatus{start(raceEngine, racer, raceConversation, "race-a", profile.ID), start(raceEngine, racer, raceConversation, "race-b", profile.ID)}
	before := calls.Load()
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j model.JobStatus) { defer wg.Done(); _, err := run(raceEngine, racer, j, j.ID); results <- err }(j)
	}
	wg.Wait()
	close(results)
	success, limited := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, model.ErrQuota) {
			limited++
		} else {
			t.Fatal("concurrent admission", err)
		}
	}
	if success != 1 || limited != 1 || calls.Load() != before+1 {
		t.Fatal("concurrent cost overspend", success, limited, calls.Load()-before)
	}
	usage(racer.Tenant, 100, 6)
	upgraded, upgradeConversation := setup("cost-upgrade")
	unmetered := start(disabled, upgraded, upgradeConversation, "unmetered", unpriced.ID)
	if _, err = run(disabled, upgraded, unmetered, "unmetered-run"); err != nil {
		t.Fatal(err)
	}
	priced := start(increased, upgraded, upgradeConversation, "priced", profile.ID)
	if _, err = run(increased, upgraded, priced, "priced-run"); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("unpriced spend hidden during activation", err)
	}
	if _, err = admin.Exec(ctx, `UPDATE extraction_quota SET day=(clock_timestamp() AT TIME ZONE 'UTC')::date-1 WHERE tenant_id=$1`, upgraded.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = run(increased, upgraded, priced, "priced-run"); err != nil {
		t.Fatal("fresh-day cost activation", err)
	}
	usage(upgraded.Tenant, 100, 6)
	maximal := configured(profile, math.MaxInt64)
	overflow := start(maximal, a, conversation, "overflow", profile.ID)
	if _, err = admin.Exec(ctx, `UPDATE extraction_quota SET reserved_cost_units=$2 WHERE tenant_id=$1`, a.Tenant, int64(math.MaxInt64-3)); err != nil {
		t.Fatal(err)
	}
	if _, err = run(maximal, a, overflow, "overflow-run"); !errors.Is(err, model.ErrQuota) {
		t.Fatal("cost arithmetic overflow", err)
	}
	usage(a.Tenant, 100, math.MaxInt64-3)
}
