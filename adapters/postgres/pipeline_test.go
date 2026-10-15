package postgres_test

import (
	"bytes"
	"connectrpc.com/connect/v2"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/archive"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"os"
	"testing"
	"time"
)

type pipelineEmbedder struct{}

func (pipelineEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0, 0}, nil
}
func testPipeline(t *testing.T, ctx context.Context, l *postgres.Ledger, store *evidence.Postgres, admin *pgxpool.Pool) {
	dsn := os.Getenv("TEST_PIPELINE_DATABASE_URL")
	if dsn == "" {
		t.Skip("set a fresh disposable pgvector pipeline database")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	config := cortex.Config{Tenant: "bank1", Generation: "g1", EmbeddingID: "test-constant-v1", Dimensions: 3}
	if err := cortex.Provision(ctx, db, config); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='pipeline_runtime') THEN CREATE ROLE pipeline_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$; GRANT USAGE ON SCHEMA public TO pipeline_runtime; GRANT SELECT ON collections,facts_projection_identity,facts_projection_tombstones TO pipeline_runtime; GRANT SELECT,INSERT,UPDATE ON embeddings TO pipeline_runtime; GRANT SELECT,UPDATE ON facts_projection_gate TO pipeline_runtime; GRANT SELECT,INSERT ON facts_projection_receipts TO pipeline_runtime`); err != nil {
		t.Fatal(err)
	}
	runtime, err := sql.Open("pgx", dsn+"&options=-c%20role%3Dpipeline_runtime")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	runtime.SetMaxOpenConns(4)
	index, err := cortex.New(ctx, runtime, config, pipelineEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	source := func(role string) *postgres.Ledger {
		t.Helper()
		cfg := admin.Config().Copy()
		cfg.ConnConfig.RuntimeParams["role"] = role
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		cipher, err := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
		if err != nil {
			t.Fatal(err)
		}
		ledger, err := postgres.New(ctx, pool, cipher)
		if err != nil {
			t.Fatal(err)
		}
		return ledger
	}
	spanExporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spanExporter))
	defer func() { _ = tracerProvider.Shutdown(context.Background()) }()
	workerTelemetry := model.TelemetryOptions{TracerProvider: tracerProvider}
	projector := source("facts_projector")
	c := model.Caller{Tenant: "bank1", Principal: "index-worker"}
	handler, err := workers.NewProjectionWithTelemetry(projector, index, allow{}, c, "g1", workerTelemetry)
	if err != nil {
		t.Fatal(err)
	}
	key := model.ConsumerKey{Tenant: c.Tenant, Consumer: model.ProjectionConsumer("g1")}
	if err := projector.RegisterConsumer(ctx, key, 3); err != nil {
		t.Fatal(err)
	}
	options := workers.Options{Lease: time.Minute, PollInterval: time.Millisecond, MinBackoff: time.Second, MaxBackoff: time.Minute}
	worker, err := workers.NewOutbox(projector, key, handler, options)
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = meterProvider.Shutdown(context.Background()) }()
	filtered := func() int64 {
		t.Helper()
		var data metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &data); err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, scope := range data.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name == "facts.recall.filtered" {
					for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
						total += point.Value
					}
				}
			}
		}
		return total
	}
	engine, err := facts.NewWithOptions(l, allow{}, store, model.EngineOptions{Telemetry: model.TelemetryOptions{MeterProvider: meterProvider}, Retrieval: model.RetrievalOptions{Index: index, Progress: l, Generation: "g1"}})
	if err != nil {
		t.Fatal(err)
	}
	testConversations(t, ctx, l, store, c)
	testExtraction(t, ctx, l, store, admin, projector, c)
	testMemoryRefresh(t, ctx, l, store, projector, c)
	query := model.RecallRequest{Scopes: []string{"case1"}, Query: "timeline", Purpose: "review", ValidAt: time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC), Limit: 20, RequireRevision: 1}
	if _, err := engine.Recall(ctx, c, query); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("coverage requirement bypassed", err)
	}
	initialProgress, err := projector.ReadOutboxProgress(ctx, key)
	if err != nil || initialProgress.LedgerRevision <= initialProgress.AcknowledgedRevision || initialProgress.OldestRecordedAt == nil {
		t.Fatal("missing projection lag", initialProgress, err)
	}
	monitor, err := workers.NewProgressMonitor(projector, []workers.ProgressTarget{{Key: key, Kind: "projection"}}, meterProvider, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = monitor.Close() }()
	assertLagMetric := func(want int64) {
		t.Helper()
		var data metricdata.ResourceMetrics
		if err := reader.Collect(ctx, &data); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, scope := range data.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name == "facts.outbox.pending_revisions" {
					for _, point := range m.Data.(metricdata.Gauge[int64]).DataPoints {
						found = true
						if point.Value != want {
							t.Fatal("incorrect pending revisions", point.Value, want)
						}
					}
				}
			}
		}
		if !found {
			t.Fatal("missing progress gauge")
		}
	}
	assertLagMetric(initialProgress.LedgerRevision - initialProgress.AcknowledgedRevision)
	drain := func(w *workers.Worker, ledger *postgres.Ledger, key model.ConsumerKey) {
		t.Helper()
		for i := 0; i < 1000; i++ {
			worked, err := w.ProcessOne(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !worked {
				s, err := ledger.ConsumerStatus(ctx, key)
				if err != nil || s.State != "pending" || s.LastFailureCode != "" {
					t.Fatal("consumer did not converge", s, err)
				}
				snapshot, err := ledger.ReadOutboxProgress(ctx, key)
				if err != nil || snapshot.LedgerRevision != snapshot.AcknowledgedRevision || snapshot.OldestRecordedAt != nil {
					t.Fatal("drained progress inconsistent", snapshot, err)
				}
				return
			}
		}
		t.Fatal("unbounded delivery")
	}
	waitRequest := model.ProjectionWaitRequest{Scopes: query.Scopes, Generation: "g1", Revision: 1, Purpose: query.Purpose}
	shortWait, stopShortWait := context.WithTimeout(ctx, 20*time.Millisecond)
	_, waitErr := engine.WaitForProjection(shortWait, c, waitRequest)
	stopShortWait()
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatal("wait ignored lag", waitErr)
	}
	waitContext, stopWait := context.WithTimeout(ctx, 10*time.Second)
	defer stopWait()
	waitDone := make(chan error, 1)
	go func() { _, err := engine.WaitForProjection(waitContext, c, waitRequest); waitDone <- err }()
	drain(worker, projector, key)
	assertLagMetric(0)
	if err := <-waitDone; err != nil {
		t.Fatal("projection wait did not converge", err)
	}
	wrongGeneration := query
	wrongGeneration.RequireGeneration = "other-generation"
	if _, err := engine.Recall(ctx, c, wrongGeneration); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("generation requirement bypassed", err)
	}
	testRemoteRecall(t, ctx, engine, c, wrongGeneration, connect.CodeUnavailable)
	query.RequireGeneration = "g1"
	result, err := engine.Recall(ctx, c, query)
	if err != nil || result.Incomplete || len(result.Items) == 0 {
		t.Fatal(result, err)
	}
	lexicalEngine, err := facts.NewWithOptions(l, allow{}, store, model.EngineOptions{Telemetry: model.TelemetryOptions{MeterProvider: meterProvider}, Retrieval: model.RetrievalOptions{Index: index.Lexical(), Progress: l, Generation: "g1"}})
	if err != nil {
		t.Fatal(err)
	}
	testRemoteRecall(t, ctx, engine, c, query, 0)
	testMemoryRecall(t, ctx, lexicalEngine, c, query, filtered)
	lexicalManifest, err := lexicalEngine.PrepareContext(ctx, c, model.ContextRequest{Command: model.Command{Key: "lexical-context", Purpose: query.Purpose}, Scope: "case1", Query: query})
	if err != nil || len(lexicalManifest.Items) == 0 || lexicalManifest.Retrieval.EmbeddingID != "none" || lexicalManifest.Retrieval.Dimensions != 0 {
		t.Fatal("lexical context provenance", lexicalManifest, err)
	}
	testContexts(t, ctx, engine, l, store, c, index, query, admin)
	drain(worker, projector, key)
	result, err = engine.Recall(ctx, c, query)
	if err != nil {
		t.Fatal(err)
	}
	var expected model.FactResult
	for _, item := range result.Items {
		direct, err := engine.GetFact(ctx, c, model.FactQuery{Key: item.Key, ValidAt: query.ValidAt})
		if err != nil || direct.VersionID != item.Fact.VersionID || direct.State != "resolved" {
			t.Fatal(item, direct, err)
		}
		if item.Key.Subject == "temporal-subject" {
			expected = item.Fact
		}
	}
	if expected.VersionID == "" {
		t.Fatal("canonical timeline absent", result)
	}
	// Prove stale index rows cannot override current ledger lifecycle state.
	raw, err := index.Candidates(ctx, "bank1", "g1", "case1", "timeline", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= len(result.Items) {
		t.Fatal("fixture did not retain historical index versions")
	}
	for _, item := range result.Items {
		if item.Fact.RecordID == expected.RecordID && item.Fact.VersionID != expected.VersionID {
			t.Fatal("stale version disclosed")
		}
	}
	// Corrupt index content remains untrusted; canonical returned values come from ledger.
	if _, err := db.ExecContext(ctx, `UPDATE embeddings SET content='untrusted altered index text'`); err != nil {
		t.Fatal(err)
	}
	memoryQuery := query
	memoryQuery.Query = "memory"
	memoryQuery.ValidAt = time.Now().UTC()
	memories, err := engine.RecallMemories(ctx, c, memoryQuery)
	if err != nil || len(memories.Items) != 1 || memories.Items[0].Memory.Body != "Corrected memory requiring its own validation." {
		t.Fatal("vector recall trusted altered index body", memories, err)
	}
	noEvidence, err := facts.NewWithRetrieval(l, denyPermission{permission: "can_read_evidence"}, store, model.RetrievalOptions{Index: index, Progress: l, Generation: "g1"})
	if err != nil {
		t.Fatal(err)
	}
	withheld, err := noEvidence.RecallMemories(ctx, c, memoryQuery)
	if err != nil || len(withheld.Items) != 0 {
		t.Fatal("memory evidence permission bypassed", withheld, err)
	}
	successMemory := memories.Items[0]
	if _, err := noEvidence.GetMemory(ctx, c, model.MemoryQuery{Key: successMemory.Key, ValidAt: memoryQuery.ValidAt, Purpose: "review"}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("direct memory evidence permission bypassed", err)
	}
	if _, err := noEvidence.GetMemoryVersion(ctx, c, model.MemoryVersionQuery{Scope: successMemory.Key.Scope, RecordID: successMemory.Memory.RecordID, VersionID: successMemory.Memory.VersionID, Purpose: "review"}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("memory version evidence permission bypassed", err)
	}
	again, err := engine.Recall(ctx, c, query)
	if err != nil || len(again.Items) != len(result.Items) {
		t.Fatal(again, err)
	}
	for _, item := range again.Items {
		if item.Fact.Value != nil && item.Fact.Value.Text != nil && *item.Fact.Value.Text == "untrusted altered index text" {
			t.Fatal("index text escaped")
		}
	}
	denied, _ := facts.NewWithRetrieval(l, denyPermission{permission: "can_read"}, store, model.RetrievalOptions{Index: index, Progress: l, Generation: "g1"})
	if _, err := denied.Recall(ctx, c, query); !errors.Is(err, model.ErrDenied) {
		t.Fatal("unauthorized recall", err)
	}
	testRemoteRecall(t, ctx, denied, c, query, connect.CodePermissionDenied)
	query.RequireRevision = result.LedgerRevision + 1
	if _, err := engine.Recall(ctx, c, query); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("future coverage accepted", err)
	}
	testRemoteRecall(t, ctx, engine, c, query, connect.CodeUnavailable)
	query.RequireRevision = 0
	end := query.ValidAt.Add(24 * time.Hour)
	change, err := engine.RetractFact(ctx, c, model.LifecycleChange{Approval: model.Approval{Command: model.Command{Key: "pipeline-retraction", Purpose: "review"}, Scope: "case1", RecordID: expected.RecordID, VersionID: expected.VersionID, ExpectedRevision: expected.StreamRevision}, Valid: model.Interval{From: &query.ValidAt, To: &end}, Reason: "review_correction"})
	if err != nil {
		t.Fatal(err)
	}
	beforeFiltered := filtered()
	stale, err := engine.Recall(ctx, c, query)
	if err != nil || !stale.Incomplete {
		t.Fatal("stale coverage hidden", stale, err)
	}
	if filtered() <= beforeFiltered {
		t.Fatal("stale recall exclusion not measured")
	}
	for _, item := range stale.Items {
		if item.Fact.RecordID == expected.RecordID {
			t.Fatal("retracted version disclosed from stale index")
		}
	}
	testRemoteRecall(t, ctx, engine, c, query, 0)
	query.KnowledgeRevision = result.LedgerRevision
	historical, err := engine.Recall(ctx, c, query)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range historical.Items {
		if item.Fact.VersionID == expected.VersionID {
			found = true
		}
	}
	if !found {
		t.Fatal("retained historical version missing")
	}
	testRemoteRecall(t, ctx, engine, c, query, 0)
	noHistory, _ := facts.NewWithRetrieval(l, denyPermission{permission: "can_read_history"}, store, model.RetrievalOptions{Index: index, Progress: l, Generation: "g1"})
	if _, err := noHistory.Recall(ctx, c, query); !errors.Is(err, model.ErrDenied) {
		t.Fatal("historical recall permission bypassed", err)
	}
	drain(worker, projector, key)
	query.KnowledgeRevision = 0
	query.RequireRevision = change.LedgerRevision
	result, err = engine.Recall(ctx, c, query)
	if err != nil || result.Incomplete {
		t.Fatal("projection did not catch up", result, err)
	}
	// Dedicated archiver reads immutable artifacts; external effects precede queue ack.
	archiver := source("facts_archiver")
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := audit.NewEd25519("archive-key", private)
	if err != nil {
		t.Fatal(err)
	}
	cipher, err := postgres.NewAESGCM(bytes.Repeat([]byte{8}, 32))
	if err != nil {
		t.Fatal(err)
	}
	objects, err := archive.NewFiles(t.TempDir(), cipher)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]ed25519.PublicKey{"archive-key": public}
	archiveHandler, err := workers.NewArchiveWithTelemetry(archiver, objects, signer, keys, allow{}, model.Caller{Tenant: "bank1", Principal: "archive-worker"}, "audit-v1", workerTelemetry)
	if err != nil {
		t.Fatal(err)
	}
	archiveKey := model.ConsumerKey{Tenant: "bank1", Consumer: model.ArchiveConsumer("audit-v1")}
	if err := archiver.RegisterConsumer(ctx, archiveKey, 3); err != nil {
		t.Fatal(err)
	}
	// Publication events must preserve enough identity to reconstruct the exact
	// predicate, even when several predicates share the same version label.
	page, err := engine.ListChanges(ctx, c, model.ChangesQuery{Scope: "case1", Purpose: "archive-check", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range page.Events {
		if event.Type != "register_predicate" {
			continue
		}
		payload, err := archiver.ArchivePayload(ctx, model.Delivery{Key: archiveKey, Event: event})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, entry := range payload.Entries {
			if entry.Kind == "predicate" && entry.ID == event.DefinitionPredicate+"/"+event.EntityID {
				found = true
			}
		}
		if !found {
			t.Fatal("predicate identity missing from archive", event.ID)
		}
	}
	// Simulate loss of the acknowledgement after external archive publication.
	delivery, err := archiver.ClaimDelivery(ctx, archiveKey, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := archiveHandler.Handle(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `UPDATE consumer_offsets SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND consumer_id=$2`, archiveKey.Tenant, archiveKey.Consumer); err != nil {
		t.Fatal(err)
	}
	archiveWorker, err := workers.NewOutbox(archiver, archiveKey, archiveHandler, options)
	if err != nil {
		t.Fatal(err)
	}
	drain(archiveWorker, archiver, archiveKey)
	observedStages := map[string]bool{}
	for _, span := range spanExporter.GetSpans() {
		for _, attribute := range span.Attributes {
			if string(attribute.Key) == "facts.outcome" && attribute.Value.AsString() == "ok" {
				observedStages[span.Name] = true
			}
		}
	}
	if !observedStages["project.document"] || !observedStages["archive.event"] {
		t.Fatal("successful worker stages not traced", observedStages)
	}

	progress, err := archiver.ConsumerStatus(ctx, archiveKey)
	if err != nil || progress.AcknowledgedRevision != result.LedgerRevision {
		t.Fatal(progress, err)
	}
	head, _, err := objects.Load(ctx, "bank1", archive.ManifestKey("audit-v1", progress.AcknowledgedRevision))
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.VerifyChain(ctx, objects, "bank1", "audit-v1", head, progress.AcknowledgedRevision, 1000, keys); err != nil {
		t.Fatal(err)
	}
	testContextArchivePolicy(t, ctx, l, store, index, archiveWorker, c, query)
	drain(worker, projector, key)
	t.Run("managed rebuild and cutover", func(t *testing.T) { testRebuild(t, ctx, l, source("facts_rebuild"), store, index, db) })
	t.Run("remote rebuild audit identity", func(t *testing.T) {
		var encrypted []byte
		if err := admin.QueryRow(ctx, `SELECT payload FROM projection_rebuild_events WHERE tenant_id=$1 AND generation=$2 AND operation=$3 ORDER BY fence LIMIT 1`, "bank1", "g1", "start").Scan(&encrypted); err != nil {
			t.Fatal(err)
		}
		cipher, err := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
		if err != nil {
			t.Fatal(err)
		}
		payload, err := cipher.Open("bank1", encrypted)
		if err != nil {
			t.Fatal(err)
		}
		var retained struct{ Actor model.RebuildActor }
		if err := json.Unmarshal(payload, &retained); err != nil {
			t.Fatal(err)
		}
		a := retained.Actor
		if a.Caller == nil || a.Authorization == nil || a.Caller.Principal != "operator" || a.Caller.Tenant != "bank1" || a.Authorization.Request.Caller != *a.Caller || a.Authorization.Request.Scope != "projection-admin" || a.Authorization.Request.Permission != "can_operate" || !a.Authorization.Allowed || a.Authorization.ContextDigest != model.Digest(a.Authorization.Request) {
			t.Fatal("missing retained operation authorization", a)
		}
	})
}
