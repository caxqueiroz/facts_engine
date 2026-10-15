package postgres_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var recoveryCaller = model.Caller{Tenant: "restore-drill", Principal: "proposer"}
var recoveryKey = model.FactKey{Scope: "case", Subject: "customer", Predicate: "preference"}
var recoveryAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func recoveryEngine(t *testing.T, ctx context.Context, dsn, role string) (*postgres.Ledger, *evidence.Postgres, *facts.Engine) {
	t.Helper()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["role"] = role
	pool, err := pgxpool.NewWithConfig(ctx, config)
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
	store, err := evidence.NewPostgres(pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := facts.New(ledger, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	return ledger, store, engine
}

// Invoked separately after the regular integration suite, before pg_dump.
func TestRecoverySeed(t *testing.T) {
	dsn := os.Getenv("TEST_RECOVERY_SOURCE_URL")
	if dsn == "" || os.Getenv("TEST_RECOVERY_PHASE") != "seed" {
		t.Skip("explicit disposable recovery seed required")
	}
	ctx := context.Background()
	_, store, engine := recoveryEngine(t, ctx, dsn, "facts_runtime")
	registerTestIdentities(t, ctx, engine, recoveryCaller.Tenant, "case", []string{"customer"})
	command := func(key string) model.Command { return model.Command{Key: key, Purpose: "restore_drill"} }
	if _, err := engine.RegisterAcceptancePolicy(ctx, recoveryCaller, model.PolicyRequest{Command: command("policy"), Policy: model.AcceptancePolicy{Scope: "case", ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RegisterPredicateVersion(ctx, recoveryCaller, model.PredicateRequest{Command: command("schema"), Definition: model.PredicateVersion{Scope: "case", Predicate: "preference", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(ctx, recoveryCaller.Tenant, "message", []byte("I prefer tea."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RegisterEvidence(ctx, recoveryCaller, model.EvidenceRequest{Command: command("evidence"), Evidence: model.Evidence{ID: "evidence", Scope: "case", SourceSystem: "chat", SourceObject: "message", SourceRevision: "1", Speaker: "customer", SourceClass: "customer", MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "test", SourceTime: recoveryAt, Object: ref}}); err != nil {
		t.Fatal(err)
	}
	value := "tea"
	proposal, err := engine.ProposeFact(ctx, recoveryCaller, model.FactProposal{Command: command("proposal"), Key: recoveryKey, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &recoveryAt}, Sources: []model.SourceSpan{{EvidenceID: "evidence", Start: 9, End: 12, Quote: "tea"}}})
	if err != nil {
		t.Fatal(err)
	}
	reviewer := recoveryCaller
	reviewer.Principal = "reviewer"
	if _, err := engine.ApproveFact(ctx, reviewer, model.Approval{Command: command("approval"), Scope: "case", RecordID: proposal.RecordID, VersionID: proposal.VersionID, ExpectedRevision: proposal.StreamRevision}); err != nil {
		t.Fatal(err)
	}
}

// Compare every retained public-table row, including ciphertext and queue offsets,
// before making any changes to the restored database.
func recoveryInventory(t *testing.T, ctx context.Context, dsn string) map[string]string {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(tables) < 20 {
		t.Fatal("missing restored schema")
	}
	inventory := map[string]string{}
	for _, table := range tables {
		rows, err := pool.Query(ctx, "SELECT row_to_json(t)::text FROM "+pgx.Identifier{"public", table}.Sanitize()+" t ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.New()
		count := 0
		for rows.Next() {
			var row string
			if err := rows.Scan(&row); err != nil {
				t.Fatal(err)
			}
			_, _ = fmt.Fprintf(hash, "%d:%s", len(row), row)
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		inventory[table] = fmt.Sprintf("%d:%x", count, hash.Sum(nil))
	}
	return inventory
}

func TestRecoveryRestore(t *testing.T) {
	source, restored, indexURL := os.Getenv("TEST_RECOVERY_SOURCE_URL"), os.Getenv("TEST_RECOVERY_RESTORED_URL"), os.Getenv("TEST_RECOVERY_INDEX_URL")
	if source == "" || restored == "" || indexURL == "" || os.Getenv("TEST_RECOVERY_PHASE") != "verify" {
		t.Skip("explicit restored disposable databases required")
	}
	if source == restored {
		t.Fatal("restore must use a separate database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if !reflect.DeepEqual(recoveryInventory(t, ctx, source), recoveryInventory(t, ctx, restored)) {
		t.Fatal("retained row inventory differs after restore")
	}
	_, _, original := recoveryEngine(t, ctx, source, "facts_runtime")
	ledger, store, engine := recoveryEngine(t, ctx, restored, "facts_runtime")
	query := model.FactQuery{Key: recoveryKey, ValidAt: recoveryAt, Purpose: "restore_drill"}
	want, err := original.GetFact(ctx, recoveryCaller, query)
	if err != nil {
		t.Fatal(err)
	}
	got, err := engine.GetFact(ctx, recoveryCaller, query)
	if err != nil || !reflect.DeepEqual(got, want) || got.Value == nil || got.Value.Text == nil || *got.Value.Text != "tea" {
		t.Fatal("restored accepted fact differs", err)
	}
	content, err := engine.GetEvidenceContent(ctx, recoveryCaller, model.EvidenceQuery{ID: "evidence", Scope: "case", Purpose: "restore_drill"})
	if err != nil || string(content.Bytes) != "I prefer tea." {
		t.Fatal("restored evidence cannot be verified", err)
	}
	wrongCipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{5}, 32))
	// A restored database alone cannot substitute for recovery of its encryption key.
	pool, err := pgxpool.New(ctx, restored)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	wrongStore, _ := evidence.NewPostgres(pool, wrongCipher)
	if _, err := wrongStore.Read(ctx, recoveryCaller.Tenant, content.Metadata.Object); err == nil {
		t.Fatal("wrong recovery key accepted")
	}

	db, err := sql.Open("pgx", indexURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	config := cortex.Config{Tenant: recoveryCaller.Tenant, Generation: "restored-v1", EmbeddingID: "none", Dimensions: 1}
	if err := cortex.Provision(ctx, db, config); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "GRANT USAGE ON SCHEMA public TO pipeline_runtime; GRANT SELECT ON collections,facts_projection_identity,facts_projection_tombstones TO pipeline_runtime; GRANT SELECT,INSERT,UPDATE ON embeddings TO pipeline_runtime; GRANT SELECT,UPDATE ON facts_projection_gate TO pipeline_runtime; GRANT SELECT,INSERT ON facts_projection_receipts TO pipeline_runtime"); err != nil {
		t.Fatal(err)
	}
	indexDB, err := sql.Open("pgx", indexURL+"&options=-c%20role%3Dpipeline_runtime")
	if err != nil {
		t.Fatal(err)
	}
	defer indexDB.Close()
	indexDB.SetMaxOpenConns(4)
	index, err := cortex.New(ctx, indexDB, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	projector, _, _ := recoveryEngine(t, ctx, restored, "facts_projector")
	handler, err := workers.NewProjection(projector, index, allow{}, recoveryCaller, config.Generation)
	if err != nil {
		t.Fatal(err)
	}
	key := model.ConsumerKey{Tenant: recoveryCaller.Tenant, Consumer: model.ProjectionConsumer(config.Generation)}
	if err := projector.RegisterConsumer(ctx, key, 3); err != nil {
		t.Fatal(err)
	}
	worker, err := workers.NewOutbox(projector, key, handler, workers.Options{Lease: time.Minute, PollInterval: time.Millisecond, MinBackoff: time.Millisecond, MaxBackoff: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; ; n++ {
		if n >= 100 {
			t.Fatal("restored projection failed to converge")
		}
		processed, err := worker.ProcessOne(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	progress, err := projector.ReadOutboxProgress(ctx, key)
	if err != nil || progress.AcknowledgedRevision != progress.LedgerRevision || progress.LedgerRevision == 0 {
		t.Fatal("restored ledger not fully projected", err)
	}
	retrieval, err := facts.NewWithRetrieval(ledger, allow{}, store, model.RetrievalOptions{Index: index.Lexical(), Progress: ledger, Generation: config.Generation})
	if err != nil {
		t.Fatal(err)
	}
	result, err := retrieval.Recall(ctx, recoveryCaller, model.RecallRequest{Scopes: []string{"case"}, Query: "tea", ValidAt: recoveryAt, Purpose: "restore_drill", Limit: 10, RequireRevision: progress.LedgerRevision})
	if err != nil || result.Incomplete || len(result.Items) != 1 || result.Items[0].Fact.VersionID != want.VersionID || !reflect.DeepEqual(result.Items[0].Fact.Value, want.Value) {
		t.Fatal("rebuild did not reproduce retained fact", err)
	}
}
