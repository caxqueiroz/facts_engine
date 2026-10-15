package cortex_test

import (
	"context"
	"database/sql"
	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/model"
	"os"
	"testing"
)

func TestLexicalOnlyGeneration(t *testing.T) {
	dsn := os.Getenv("TEST_LEXICAL_DATABASE_URL")
	if dsn == "" {
		t.Skip("set a fresh lexical generation database")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	config := cortex.Config{Tenant: "lexical-bank", Generation: "lexical-v1", EmbeddingID: "none", Dimensions: 1}
	if err = cortex.Provision(ctx, db, config); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='lexical_runtime') THEN CREATE ROLE lexical_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$; GRANT USAGE ON SCHEMA public TO lexical_runtime; GRANT SELECT ON collections,facts_projection_identity,facts_projection_tombstones TO lexical_runtime; GRANT SELECT,INSERT,UPDATE ON embeddings TO lexical_runtime; GRANT SELECT ON facts_projection_gate TO lexical_runtime; GRANT UPDATE(fence,sealed) ON facts_projection_gate TO lexical_runtime; GRANT SELECT,INSERT ON facts_projection_receipts TO lexical_runtime`); err != nil {
		t.Fatal(err)
	}
	runtime, err := sql.Open("pgx", dsn+"&options=-c%20role%3Dlexical_runtime")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	runtime.SetMaxOpenConns(4)
	index, err := cortex.New(ctx, runtime, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	value := "customer prefers jasmine tea"
	doc := model.ProjectionDocument{Tenant: config.Tenant, Generation: config.Generation, RecordID: "record", VersionID: "version", VersionDigest: model.BytesDigest([]byte(value)), PolicyID: "policy", Classification: "confidential", Key: model.FactKey{Scope: "case", Subject: "subject", Predicate: "preference"}, Value: model.Value{Kind: model.Text, Text: &value}}
	if err = index.Put(ctx, doc); err != nil {
		t.Fatal(err)
	}
	hits, err := index.Candidates(ctx, config.Tenant, config.Generation, "case", "jasmine tea", 10)
	if err != nil || len(hits) != 1 || hits[0].VersionID != "version" {
		t.Fatal(hits, err)
	}
	identity, err := index.DescribeRetrieval(ctx, config.Tenant, config.Generation)
	if err != nil || identity.EmbeddingID != "none" || identity.Dimensions != 0 {
		t.Fatal(identity, err)
	}
	hits, err = index.Candidates(ctx, config.Tenant, config.Generation, "case", "coffee", 10)
	if err != nil || len(hits) != 0 {
		t.Fatal(hits, err)
	}
}
