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
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
)

func testMemoryContexts(t *testing.T, ctx context.Context, e *facts.Engine, l *postgres.Ledger, store *evidence.Postgres, c model.Caller, index model.RetrievalIndex, input, output model.ArtifactReference) model.ContextManifest {
	t.Helper()
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "review"} }
	r := model.ContextRequest{Command: cmd("mixed-context"), Scope: "case1", IncludeMemories: true, Query: model.RecallRequest{Scopes: []string{"case1"}, Query: "memory", Purpose: "review", ValidAt: time.Now().UTC(), Limit: 20}}
	m, err := e.PrepareContext(ctx, c, r)
	if err != nil || len(m.Memories) != 1 || len(m.Items) == 0 || len(m.Order) != len(m.Items)+len(m.Memories) || m.CanonicalVersion != "context-manifest-v2" {
		t.Fatal("mixed manifest", m, err)
	}
	var rendered struct {
		Schema   string
		Facts    []model.RecallItem
		Memories []model.MemoryRecallItem
		Order    []model.VersionReference
	}
	if err = json.Unmarshal(m.Rendered, &rendered); err != nil {
		t.Fatal(err)
	}
	if rendered.Schema != "facts-memories-context-json-v1" || model.Digest(rendered.Facts) != model.Digest(m.Items) || model.Digest(rendered.Memories) != model.Digest(m.Memories) || model.Digest(rendered.Order) != model.Digest(m.Order) {
		t.Fatal("rendered mixed identity mismatch")
	}
	for _, item := range m.Items {
		if item.Fact.LedgerRevision != m.SelectionRevision {
			t.Fatal("fact selection revision mismatch")
		}
	}
	for _, item := range m.Memories {
		if item.Memory.LedgerRevision != m.SelectionRevision {
			t.Fatal("memory selection revision mismatch")
		}
	}
	memory := m.Memories[0]
	ref := model.VersionReference{RecordID: memory.Memory.RecordID, VersionID: memory.Memory.VersionID}
	decision := model.DecisionReceipt{Command: cmd("mixed-decision"), Scope: "case1", ApplicationID: "app", ExternalDecisionID: "mixed", ContextID: m.ID, SelectedItems: []model.VersionReference{ref}, UsedItems: []model.VersionReference{ref}, MethodKind: "rule", MethodID: "memory-rule", MethodVersion: "v1", TransformationVersion: "v1", Input: input, Output: output, DecidedAt: time.Now(), Outcome: "succeeded"}
	retained, err := e.RecordDecision(ctx, c, decision)
	if err != nil || retained.SourceDigest != m.Digest {
		t.Fatal("memory decision", retained, err)
	}
	invalid := decision
	invalid.Command = cmd("mixed-decision-invalid")
	invalid.ExternalDecisionID = "mixed-invalid"
	invalid.SelectedItems = []model.VersionReference{{RecordID: ref.RecordID, VersionID: "unprovided-version"}}
	invalid.UsedItems = invalid.SelectedItems
	if _, err = e.RecordDecision(ctx, c, invalid); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("unprovided memory used", err)
	}
	for _, permission := range []string{"can_read_history", "can_read_evidence"} {
		denied, err := facts.New(l, denyPermission{permission: permission}, store)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = denied.GetContextManifest(ctx, c, model.ContextQuery{ID: m.ID, Purpose: "review"}); !errors.Is(err, model.ErrDenied) {
			t.Fatal("memory manifest permission bypass", permission, err)
		}
		if _, err = denied.GetDecisionReceipt(ctx, c, model.DecisionQuery{ID: retained.ID, Purpose: "review"}); !errors.Is(err, model.ErrDenied) {
			t.Fatal("memory decision permission bypass", permission, err)
		}
	}
	change := model.LifecycleChange{Approval: model.Approval{Command: cmd("context-memory-dispute"), Scope: memory.Key.Scope, RecordID: ref.RecordID, VersionID: ref.VersionID, ExpectedRevision: memory.Memory.StreamRevision}, Valid: model.Interval{From: &r.Query.ValidAt}, Reason: "review"}
	disputed, err := e.DisputeMemory(ctx, c, change)
	if err != nil {
		t.Fatal(err)
	}
	broken, err := facts.NewWithRetrieval(l, allow{}, store, model.RetrievalOptions{Index: unavailableIndex{index}, Progress: l, Generation: "g1"})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := broken.PrepareContext(ctx, c, r)
	if err != nil || retry.ID != m.ID || !bytes.Equal(retry.Rendered, m.Rendered) {
		t.Fatal("mixed retry changed supplied bytes", err)
	}
	fresh := r
	fresh.Command = cmd("mixed-context-after-dispute")
	after, err := e.PrepareContext(ctx, c, fresh)
	if err != nil || len(after.Memories) != 0 {
		t.Fatal("disputed memory entered new context", after, err)
	}
	if _, err = e.GetDecisionReceipt(ctx, c, model.DecisionQuery{ID: retained.ID, Purpose: "review"}); err != nil {
		t.Fatal("lifecycle rewrote decision source", err)
	}
	change.Command = cmd("context-memory-resolve")
	change.ExpectedRevision = disputed.StreamRevision
	if _, err = e.ResolveMemoryDispute(ctx, c, change); err != nil {
		t.Fatal(err)
	}
	return m
}
