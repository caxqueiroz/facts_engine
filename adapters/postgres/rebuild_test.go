package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"os"
	"sync"
	"testing"
	"time"
)

// Block exactly the old generation candidate query to force a cutover during recall.
type cutoverIndex struct {
	model.RetrievalIndex
	entered, release chan struct{}
	once             sync.Once
}

func (i *cutoverIndex) Candidates(ctx context.Context, t, g, s, q string, n int) ([]model.SearchCandidate, error) {
	if g == "g1" {
		i.once.Do(func() { close(i.entered) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-i.release:
		}
	}
	return i.RetrievalIndex.Candidates(ctx, t, g, s, q, n)
}

type failRetirement struct {
	model.RebuildIndex
	failed bool
}

func (i *failRetirement) SetProjectionGate(ctx context.Context, k model.GenerationKey, f int64, sealed bool) error {
	if k.Generation == "g1" && sealed && !i.failed {
		i.failed = true
		return model.ErrUnavailable
	}
	return i.RebuildIndex.SetProjectionGate(ctx, k, f, sealed)
}
func testRebuild(t *testing.T, ctx context.Context, l, registry *postgres.Ledger, store *evidence.Postgres, old *cortex.Index, oldAdmin *sql.DB) {
	dsn := os.Getenv("TEST_REBUILD_DATABASE_URL")
	if dsn == "" {
		t.Skip("set a fresh pgvector rebuild database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	config := cortex.Config{Tenant: "org1", Generation: "g2", EmbeddingID: "test-constant-v1", Dimensions: 3}
	if err := cortex.Provision(ctx, db, config); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `GRANT USAGE ON SCHEMA public TO pipeline_runtime; GRANT SELECT ON collections,facts_projection_identity,facts_projection_tombstones TO pipeline_runtime; GRANT SELECT,INSERT,UPDATE ON embeddings TO pipeline_runtime; GRANT SELECT,UPDATE ON facts_projection_gate TO pipeline_runtime; GRANT SELECT,INSERT ON facts_projection_receipts TO pipeline_runtime`); err != nil {
		t.Fatal(err)
	}
	runtime, err := sql.Open("pgx", dsn+"&options=-c%20role%3Dpipeline_runtime")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	runtime.SetMaxOpenConns(4)
	next, err := cortex.New(ctx, runtime, config, pipelineEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	routes, err := cortex.NewGenerations(old, next)
	if err != nil {
		t.Fatal(err)
	}
	actor := model.RebuildActor{Principal: "operator", Purpose: "verified_rebuild"}
	manager := newRemoteRebuild(t, ctx, l, registry, store, routes)
	oldKey := model.GenerationKey{Tenant: "org1", Generation: "g1"}
	key := model.GenerationKey{Tenant: "org1", Generation: "g2"}
	c := model.Caller{Tenant: "org1", Principal: "reader"}
	query := model.RecallRequest{Scopes: []string{"case1"}, Query: "timeline", Purpose: "review", ValidAt: time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC), Limit: 20}
	managed, err := facts.NewWithRetrieval(l, allow{}, store, model.RetrievalOptions{Index: routes, Progress: l, Registry: l})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := managed.Recall(ctx, c, query); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("unregistered generation exposed", err)
	}
	if _, err := l.StartRebuild(ctx, oldKey, actor, 3); !errors.Is(err, model.ErrDenied) {
		t.Fatal("runtime role managed a rebuild", err)
	}
	if _, err := manager.Start(ctx, oldKey, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Start(ctx, oldKey, 4); !errors.Is(err, model.ErrConflict) {
		t.Fatal("retry changed budget", err)
	}
	// The preceding pipeline test deliberately corrupted index text. A successful
	// consumer offset alone must not make that generation publishable.
	if _, err := manager.Verify(ctx, oldKey); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("corrupt canonical text verified", err)
	}
	status, err := registry.RebuildStatus(ctx, oldKey)
	if err != nil || status.State != "paused" {
		t.Fatal(status, err)
	}
	docs, err := registry.ProjectionDocuments(ctx, oldKey, status.ThroughRevision)
	if err != nil {
		t.Fatal(err)
	}
	// Exhaust the frozen source through small pages; no accepted version may
	// disappear or repeat at the fact/memory boundary.
	for _, limit := range []int{1, 3, model.MaxProjectionPageDocuments} {
		cursor := model.ProjectionCursor{}
		seen := map[string]bool{}
		for {
			page, err := registry.ProjectionDocumentPage(ctx, oldKey, status.Fence, status.ThroughRevision, cursor, limit)
			if err != nil || len(page.Documents) > limit {
				t.Fatal("source page", page, err)
			}
			for _, document := range page.Documents {
				id := model.ProjectionID(document)
				if seen[id] {
					t.Fatal("duplicate paged document", id)
				}
				seen[id] = true
			}
			if page.Next == nil {
				break
			}
			if !cursor.Before(*page.Next) {
				t.Fatal("cursor failed to advance")
			}
			cursor = *page.Next
		}
		if len(seen) != len(docs) {
			t.Fatal("incomplete paged inventory", len(seen), len(docs))
		}
		for _, document := range docs {
			if !seen[model.ProjectionID(document)] {
				t.Fatal("missing paged document")
			}
		}
	}
	if _, err := registry.ProjectionDocumentPage(ctx, oldKey, status.Fence+1, status.ThroughRevision, model.ProjectionCursor{}, 1); !errors.Is(err, model.ErrRevision) {
		t.Fatal("stale source fence", err)
	}
	if _, err := registry.ProjectionDocumentPage(ctx, oldKey, status.Fence, status.ThroughRevision+1, model.ProjectionCursor{}, 1); !errors.Is(err, model.ErrRevision) {
		t.Fatal("changed source revision", err)
	}
	for _, d := range docs {
		raw, err := json.Marshal(struct {
			Key   model.FactKey
			Value model.Value
		}{d.Key, d.Value})
		if d.Memory != nil {
			raw, err = json.Marshal(d.Memory)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := oldAdmin.ExecContext(ctx, `UPDATE embeddings SET content=$2 WHERE id=$1`, model.ProjectionID(d), string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	legacyCheck, err := old.VerifyProjection(ctx, oldKey, status.Fence, docs)
	if err != nil {
		t.Fatal("legacy verifier", err)
	}
	legacyVerified, err := registry.VerifyRebuild(ctx, oldKey, actor, status.Fence, legacyCheck)
	if err != nil || legacyVerified.VerificationVersion != "" {
		t.Fatal("legacy proof", legacyVerified, err)
	}
	legacyRetry, err := manager.Verify(ctx, oldKey)
	if err != nil || legacyRetry != legacyVerified {
		t.Fatal("legacy retry changed digest contract", legacyRetry, err)
	}
	if _, err := manager.Resume(ctx, oldKey, legacyVerified.Fence); err != nil {
		t.Fatal("resume legacy proof", err)
	}
	verified, err := manager.Verify(ctx, oldKey)
	if err != nil || verified.Count != len(docs) || verified.VerificationVersion != model.ProjectionInventoryV2 {
		t.Fatal(verified, err)
	}
	repeated, err := manager.Verify(ctx, oldKey)
	if err != nil || repeated != verified {
		t.Fatal("paged verify retry", repeated, err)
	}
	initial, err := manager.Activate(ctx, oldKey, verified.Fence)
	if err != nil || initial.State != "active" {
		t.Fatal(initial, err)
	}
	if _, err := manager.Activate(ctx, oldKey, verified.Fence); err != nil {
		t.Fatal("activation response loss", err)
	}
	active, err := l.ActiveProjection(ctx, "org1")
	if err != nil || active.Generation != "g1" || active.Epoch != 1 {
		t.Fatal(active, err)
	}
	started, err := manager.Start(ctx, key, 3)
	if err != nil || started.BaseEpoch != 1 {
		t.Fatal(started, err)
	}
	if _, err := manager.Verify(ctx, key); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("lagging generation verified", err)
	}
	if _, err := manager.Activate(ctx, key, started.Fence); !errors.Is(err, model.ErrRevision) {
		t.Fatal("unverified cutover", err)
	}
	handler, err := workers.NewProjection(registry, routes, allow{}, model.Caller{Tenant: "org1", Principal: "projector"}, "g2")
	if err != nil {
		t.Fatal(err)
	}
	queueKey := model.ConsumerKey{Tenant: "org1", Consumer: model.ProjectionConsumer("g2")}
	worker, err := workers.NewOutbox(registry, queueKey, handler, workers.Options{Lease: time.Minute, PollInterval: time.Millisecond, MinBackoff: time.Second, MaxBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	drain := func() {
		t.Helper()
		for n := 0; n < 1000; n++ {
			worked, err := worker.ProcessOne(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !worked {
				s, err := registry.ConsumerStatus(ctx, queueKey)
				if err != nil || s.State != "pending" || s.LastFailureCode != "" {
					t.Fatal(s, err)
				}
				return
			}
		}
		t.Fatal("replay did not converge")
	}
	drain()
	// Extra physical rows must fail even when all expected receipts are present.
	if _, err := db.ExecContext(ctx, `INSERT INTO embeddings(id,collection_id,vector,content,metadata) SELECT 'unexpected-row',collection_id,vector,content,metadata FROM embeddings LIMIT 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Verify(ctx, key); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("extra row accepted", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM embeddings WHERE id='unexpected-row'`); err != nil {
		t.Fatal(err)
	}
	verified, err = manager.Verify(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	// A new ledger commit invalidates cutover without blocking normal writers.
	err = l.Transact(ctx, "org1", func(tx model.Tx) error {
		_, err := tx.Append(ctx, model.Event{ID: "during-rebuild", Type: "rebuild-test-change", Scope: "case1", Caller: c})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Activate(ctx, key, verified.Fence); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("stale verification cutover", err)
	}
	resumed, err := manager.Resume(ctx, key, verified.Fence)
	if err != nil || resumed.State != "building" {
		t.Fatal(resumed, err)
	}
	if _, err := registry.VerifyRebuild(ctx, key, actor, verified.Fence, model.ProjectionCheck{Count: verified.Count, Digest: verified.Digest}); !errors.Is(err, model.ErrRevision) {
		t.Fatal("stale verification receipt", err)
	}
	if err := routes.SetProjectionGate(ctx, key, verified.Fence, true); !errors.Is(err, model.ErrRevision) {
		t.Fatal("stale destination gate", err)
	}
	drain()
	verified, err = manager.Verify(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	// Keep an old-generation delivery in flight while switching. Its completion
	// must be fenced, even if external work had already begun.
	oldDelivery, err := registry.ClaimDelivery(ctx, model.ConsumerKey{Tenant: "org1", Consumer: model.ProjectionConsumer("g1")}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	blocked := &cutoverIndex{RetrievalIndex: routes, entered: make(chan struct{}), release: make(chan struct{})}
	crossing, err := facts.NewWithRetrieval(l, allow{}, store, model.RetrievalOptions{Index: blocked, Progress: l, Registry: l})
	if err != nil {
		t.Fatal(err)
	}
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := crossing.Recall(callCtx, c, query); result <- err }()
	select {
	case <-blocked.entered:
	case <-callCtx.Done():
		t.Fatal("recall did not reach index")
	}
	failing := &failRetirement{RebuildIndex: routes}
	interrupted, err := workers.NewRebuild(registry, failing, actor)
	if err != nil {
		t.Fatal(err)
	}
	switched, err := interrupted.Activate(ctx, key, verified.Fence)
	if !errors.Is(err, model.ErrUnavailable) || switched.State != "activating" {
		t.Fatal(switched, err)
	}
	close(blocked.release)
	if err := <-result; !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("retired request returned across cutover", err)
	}
	active, err = l.ActiveProjection(ctx, "org1")
	if err != nil || active.Generation != "g2" || active.Epoch != 2 {
		t.Fatal(active, err)
	}
	if err := registry.AcknowledgeDelivery(ctx, oldDelivery); !errors.Is(err, model.ErrRevision) {
		t.Fatal("retired worker acknowledged", err)
	}
	// Resume after a crash between pointer switch and destination gate cleanup.
	complete, err := manager.Activate(ctx, key, verified.Fence)
	if err != nil || complete.State != "active" {
		t.Fatal(complete, err)
	}
	for n := 0; n < 2; n++ {
		if _, err := manager.Activate(ctx, key, verified.Fence); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := registry.ClaimDelivery(ctx, oldDelivery.Key, time.Minute); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("retired consumer claimed work", err)
	}
	retired, err := registry.RebuildStatus(ctx, oldKey)
	if err != nil || retired.State != "retired" {
		t.Fatal(retired, err)
	}
	if _, err := manager.Resume(ctx, oldKey, retired.Fence); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("retired generation resumed", err)
	}
	if err := routes.SetProjectionGate(ctx, oldKey, retired.Fence-1, false); !errors.Is(err, model.ErrRevision) {
		t.Fatal("retired gate reopened", err)
	}
	if len(docs) == 0 {
		t.Fatal("expected historical versions")
	}
	stale := docs[0]
	stale.VersionID = "late-stale-worker-version"
	if err := old.Put(ctx, stale); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("retired destination accepted stale write", err)
	}
	response, err := managed.Recall(ctx, c, query)
	if err != nil || response.Generation != "g2" || response.Incomplete {
		t.Fatal(response, err)
	}
	if _, err := manager.Abandon(ctx, key, complete.Fence); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("active generation abandoned", err)
	}
	unusedKey := model.GenerationKey{Tenant: "org1", Generation: "unused"}
	unused, err := registry.StartRebuild(ctx, unusedKey, actor, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Abandon(ctx, unusedKey, unused.Fence); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("unprovisioned destination gate should remain unavailable", err)
	}
	abandoned, err := registry.RebuildStatus(ctx, unusedKey)
	if err != nil || abandoned.State != "abandoned" {
		t.Fatal(abandoned, err)
	}
	if _, err := registry.AbandonRebuild(ctx, unusedKey, actor, unused.Fence); err != nil {
		t.Fatal("abandon response loss", err)
	}
	if _, err := registry.FreezeRebuild(ctx, unusedKey, actor); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("abandoned generation resumed", err)
	}
	if _, err := registry.RebuildStatus(ctx, model.GenerationKey{Tenant: "another-tenant", Generation: "g2"}); !errors.Is(err, model.ErrNotFound) {
		t.Fatal("cross-tenant rebuild exposed", err)
	}
}
