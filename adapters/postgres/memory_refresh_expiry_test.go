package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
)

func testExpiryMemoryRefresh(t *testing.T, ctx context.Context, l model.Ledger, store *evidence.Postgres, queue model.Outbox, c, reviewer, worker model.Caller, profile model.ExtractionProfile, proposal model.FactProposal) {
	t.Helper()
	scope := proposal.Key.Scope
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "expiry-review"} }
	profile.ID = "expiry-refresh"
	provider := extractionFunc(func(context.Context, []byte) (model.ExtractionResponse, error) {
		body, _ := json.Marshal(model.MemoryRefreshOutput{DeclineReason: "Dependency requires renewed governed validation."})
		return model.ExtractionResponse{ModelIdentity: "fixture-v1", Body: body, InputTokens: 1, OutputTokens: 1}, nil
	})
	engine, err := facts.NewWithOptions(l, allow{}, store, model.EngineOptions{Extraction: model.ExtractionOptions{Profiles: []model.ApprovedExtractor{{Profile: profile, Provider: provider}}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 100000}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: cmd("expiry-policy"), Policy: model.AcceptancePolicy{Scope: scope, ID: "expiring", Mode: "manual", SourceClasses: []string{"customer"}, Required: []model.AttestationRequirement{{Kind: "application_validation", Principals: []string{c.Principal}}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = engine.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: cmd("expiry-predicate"), Definition: model.PredicateVersion{Scope: scope, Predicate: "expiring-preference", Version: "v1", PolicyID: "expiring", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	proposal.Command = cmd("expiry-fact")
	proposal.Key.Predicate = "expiring-preference"
	proposal.ExpectedRevision = 0
	fact, err := engine.ProposeFact(ctx, c, proposal)
	if err != nil {
		t.Fatal(err)
	}
	expiry := time.Now().UTC().Add(10 * time.Second)
	if _, err = engine.RecordAttestation(ctx, c, model.AttestationRequest{Command: cmd("expiry-attestation"), Attestation: model.Attestation{ID: "expiring-validation", Scope: scope, RecordID: fact.RecordID, VersionID: fact.VersionID, ContentDigest: fact.ContentDigest, Kind: "application_validation", EvidenceID: proposal.Sources[0].EvidenceID, Attestor: c, ExpiresAt: &expiry}}); err != nil {
		t.Fatal(err)
	}
	if _, err = engine.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("expiry-fact-approve"), Scope: scope, RecordID: fact.RecordID, VersionID: fact.VersionID, ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	m := model.MemoryProposal{Command: cmd("expiry-memory"), Key: model.MemoryKey{Scope: scope, ID: "expiry-summary"}, Subtype: "summary", SchemaVersion: "v1", Body: "Validated customer preference.", Valid: proposal.Valid, Sources: proposal.Sources, Dependencies: []model.MemoryDependency{{Scope: scope, RecordID: fact.RecordID, VersionID: fact.VersionID, ContentDigest: fact.ContentDigest, Label: "assertion"}}}
	memory, err := engine.ProposeMemory(ctx, c, m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.ApproveMemory(ctx, reviewer, model.Approval{Command: cmd("expiry-memory-approve"), Scope: scope, RecordID: memory.RecordID, VersionID: memory.VersionID, ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	binding := []model.MemoryRefreshBinding{{Scope: scope, ProfileID: profile.ID}}
	ticker, err := workers.NewMemoryRefreshTicker(engine, worker, "expiry-v1", "expiry-review", binding, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := workers.NewMemoryRefresh(engine, worker, "expiry-v1", "expiry-review", binding)
	if err != nil {
		t.Fatal(err)
	}
	key := model.ConsumerKey{Tenant: c.Tenant, Consumer: model.MemoryRefreshConsumer("expiry-v1")}
	if err = queue.RegisterConsumer(ctx, key, 3); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workers.NewOutbox(queue, key, scheduler, workers.Options{Lease: time.Minute, PollInterval: time.Millisecond, MinBackoff: time.Second, MaxBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	drain := func() {
		t.Helper()
		for i := 0; i < 1000; i++ {
			worked, err := dispatcher.ProcessOne(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !worked {
				status, err := queue.ConsumerStatus(ctx, key)
				if err != nil || status.State != "pending" || status.LastFailureCode != "" {
					t.Fatal(status, err)
				}
				return
			}
		}
		t.Fatal("unbounded expiry dispatch")
	}
	beforeTick, err := ticker.Tick(ctx, time.Now().Add(-time.Minute))
	if err != nil || len(beforeTick) != 1 {
		t.Fatal(beforeTick, err)
	}
	drain()
	before, err := engine.GetMemory(ctx, c, model.MemoryQuery{Key: m.Key, ValidAt: *m.Valid.From, Purpose: "expiry-review"})
	if err != nil || before.State != "resolved" {
		t.Fatal("memory was not eligible before expiry", before, err)
	}
	timer := time.NewTimer(time.Until(expiry.Add(20 * time.Millisecond)))
	select {
	case <-ctx.Done():
		timer.Stop()
		t.Fatal(ctx.Err())
	case <-timer.C:
	}
	expired, err := engine.GetMemory(ctx, c, model.MemoryQuery{Key: m.Key, ValidAt: *m.Valid.From, Purpose: "expiry-review"})
	if err != nil || expired.State != "stale" || expired.LedgerRevision != before.LedgerRevision {
		t.Fatal("time-only expiry fixture changed ledger", expired, err)
	}
	at := time.Now()
	ticks, err := ticker.Tick(ctx, at)
	if err != nil || len(ticks) != 1 {
		t.Fatal(ticks, err)
	}
	repeated, err := ticker.Tick(ctx, at)
	if err != nil || repeated[0] != ticks[0] {
		t.Fatal("tick retry appended a second sweep", repeated, err)
	}
	drain()
	changes, err := engine.ListChanges(ctx, c, model.ChangesQuery{Scope: scope, Purpose: "expiry-review", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var job model.ExtractionJob
	count := 0
	for _, event := range changes.Events {
		if event.Type != "start_memory_refresh" {
			continue
		}
		var candidate model.ExtractionJob
		err = l.Transact(ctx, c.Tenant, func(tx model.Tx) error {
			var err error
			candidate, err = tx.(model.ExtractionTx).ExtractionJob(ctx, event.EntityID)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if candidate.Request.Refresh.RecordID == memory.RecordID {
			job = candidate
			count++
		}
	}
	if count != 1 || job.Request.RefreshTrigger == nil || job.Request.RefreshTrigger.EventID != ticks[0].ID {
		t.Fatal("expiry sweep failed to schedule exact target", job, count)
	}
	completed, err := engine.RunExtraction(ctx, worker, model.ExtractionRunRequest{Command: cmd("expiry-run"), ID: job.ID, Scope: scope})
	if err != nil || completed.State != "completed" {
		t.Fatal(completed, err)
	}
	retained, err := engine.GetExtractionRun(ctx, c, model.ExtractionRunQuery{ID: completed.AttemptID, Scope: scope, Purpose: "expiry-review"})
	if err != nil || retained.Result == nil || len(retained.Result.Proposals) != 0 || len(retained.Result.Issues) != 1 || retained.Result.Issues[0].Code != "refresh_declined" {
		t.Fatal("expiry manufactured an eligible replacement", retained, err)
	}
}
