package postgres_test

import (
	"context"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/model"
)

func testMemoryRecall(t *testing.T, ctx context.Context, engine *facts.Engine, caller model.Caller, query model.RecallRequest) {
	t.Helper()
	query.Query = "preference"
	// Reused below as a PostgreSQL lifecycle interval boundary.
	query.ValidAt = time.Now().UTC().Truncate(time.Microsecond)
	result, err := engine.RecallMemories(ctx, caller, query)
	if err != nil || len(result.Items) != 1 || result.Items[0].Key.ID != "memory-auto" || result.Items[0].Memory.Body != "Customer discussed a preference for tea." {
		t.Fatal("canonical memory recall", result, err)
	}
	testRemoteRecall(t, ctx, engine, caller, query, 0)
	selected := result.Items[0]
	factsResult, err := engine.Recall(ctx, caller, query)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range factsResult.Items {
		if item.Fact.RecordID == selected.Memory.RecordID {
			t.Fatal("memory escaped into fact recall")
		}
	}
	_, err = engine.RetractMemory(ctx, caller, model.LifecycleChange{
		Approval: model.Approval{Command: model.Command{Key: "recall-memory-retract", Purpose: "review"}, Scope: selected.Key.Scope, RecordID: selected.Memory.RecordID, VersionID: selected.Memory.VersionID, ExpectedRevision: selected.Memory.StreamRevision},
		Valid:    model.Interval{From: &query.ValidAt}, Reason: "review_correction",
	})
	if err != nil {
		t.Fatal(err)
	}
	stale, err := engine.RecallMemories(ctx, caller, query)
	if err != nil || len(stale.Items) != 0 || !stale.Incomplete {
		t.Fatal("stale memory index disclosed body", stale, err)
	}
	testRemoteRecall(t, ctx, engine, caller, query, 0)
	query.KnowledgeRevision = result.LedgerRevision
	testRemoteRecall(t, ctx, engine, caller, query, 0)
	historical, err := engine.RecallMemories(ctx, caller, query)
	if err != nil || len(historical.Items) != 1 || historical.Items[0].Memory.Body != selected.Memory.Body {
		t.Fatal("historical memory recall", historical, err)
	}
}
