package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect/v2"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
)

func testContextArchivePolicy(t *testing.T, ctx context.Context, ledger *postgres.Ledger, store *evidence.Postgres, index model.RetrievalIndex, worker *workers.Worker, caller model.Caller, q model.RecallRequest) {
	makeEngine := func(limit int64) *facts.Engine {
		t.Helper()
		engine, err := facts.NewWithOptions(ledger, allow{}, store, model.EngineOptions{ContextArchive: &model.ContextArchiveOptions{Stream: "audit-v1", MaxPendingRevisions: limit, Progress: ledger}, Retrieval: model.RetrievalOptions{Index: index, Progress: ledger, Generation: "g1"}})
		if err != nil {
			t.Fatal(err)
		}
		return engine
	}
	drain := func() {
		t.Helper()
		for i := 0; i < 1000; i++ {
			worked, err := worker.ProcessOne(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !worked {
				return
			}
		}
		t.Fatal("archive failed to converge")
	}
	engine := makeEngine(0)
	request := model.ContextRequest{Command: model.Command{Key: "context-archive-gate", Purpose: q.Purpose}, Scope: "case1", Query: q}
	// Creating the context itself adds a pending archive revision. With a zero
	// tolerance, withhold it until the explicit archive worker catches up.
	out, err := engine.PrepareContext(ctx, caller, request)
	if !errors.Is(err, model.ErrUnavailable) || len(out.Rendered) != 0 {
		t.Fatal("unarchived context delivered", out, err)
	}
	drain()
	manifest, err := remotePrepareContext(t, ctx, engine, caller, request)
	if err != nil || manifest.Archive == nil || manifest.Archive.Stream != "audit-v1" || manifest.Archive.MaxPendingRevisions != 0 || manifest.Archive.LedgerRevision != manifest.Archive.AcknowledgedRevision {
		t.Fatal("archive receipt", manifest.Archive, err)
	}
	if _, err := engine.PutScope(ctx, caller, model.ScopeRequest{Command: model.Command{Key: "archive-gate-new-event", Purpose: q.Purpose}, Definition: model.ScopeDefinition{ID: "archive-gate-scope", Label: "Archive gate fixture"}, Reason: "integration"}); err != nil {
		t.Fatal(err)
	}
	hidden, err := engine.GetContextManifest(ctx, caller, model.ContextQuery{ID: manifest.ID, Purpose: q.Purpose})
	if !errors.Is(err, model.ErrUnavailable) || len(hidden.Rendered) != 0 {
		t.Fatal("manifest read bypassed lag", err)
	}
	if _, err := remotePrepareContext(t, ctx, engine, caller, request); connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatal("RPC replay bypassed lag", err)
	}
	threshold := makeEngine(1)
	allowed, err := threshold.GetContextManifest(ctx, caller, model.ContextQuery{ID: manifest.ID, Purpose: q.Purpose})
	if err != nil || !bytes.Equal(allowed.Rendered, manifest.Rendered) || allowed.Archive.MaxPendingRevisions != 0 {
		t.Fatal("threshold or original receipt changed", err)
	}
	if len(manifest.Items) > 0 {
		if _, err := engine.GetFact(ctx, caller, model.FactQuery{Key: manifest.Items[0].Key, Purpose: q.Purpose, ValidAt: q.ValidAt}); err != nil {
			t.Fatal("archive gate blocked direct fact", err)
		}
	}
	drain()
	replay, err := engine.PrepareContext(ctx, caller, request)
	if err != nil || replay.ID != manifest.ID || !bytes.Equal(replay.Rendered, manifest.Rendered) {
		t.Fatal("recovery lost committed context", err)
	}
}
