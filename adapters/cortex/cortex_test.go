package cortex_test

import (
	"context"
	"database/sql"
	"errors"
	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/model"
	_ "github.com/jackc/pgx/v5/stdlib"
	"os"
	"testing"
)

type embedder struct{}

func (embedder) Embed(context.Context, string) ([]float32, error) { return []float32{1, 0, 0}, nil }
func TestPostgresContract(t *testing.T) {
	dsn := os.Getenv("TEST_CORTEX_DATABASE_URL")
	dsn2 := os.Getenv("TEST_CORTEX_OTHER_DATABASE_URL")
	if dsn == "" || dsn2 == "" {
		t.Skip("set fresh disposable CortexDB PostgreSQL databases")
	}
	ctx := context.Background()
	setup := func(dsn, tenant string) (*cortex.Index, *sql.DB) {
		t.Helper()
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		db.SetMaxOpenConns(4)
		c := cortex.Config{Tenant: tenant, Generation: "g1", EmbeddingID: "test-constant-v1", Dimensions: 3}
		if err := cortex.Provision(ctx, db, c); err != nil {
			t.Fatal(err)
		}
		if err := cortex.Provision(ctx, db, c); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("existing database reprovisioned", err)
		}
		if _, err := cortex.New(ctx, db, c, embedder{}); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("owner allowed at runtime", err)
		}
		if _, err := db.ExecContext(ctx, `DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='cortex_test_runtime') THEN CREATE ROLE cortex_test_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$; GRANT USAGE ON SCHEMA public TO cortex_test_runtime; GRANT SELECT ON collections,facts_projection_identity TO cortex_test_runtime; GRANT SELECT,INSERT,UPDATE ON embeddings TO cortex_test_runtime; GRANT SELECT,UPDATE ON facts_projection_gate TO cortex_test_runtime; GRANT SELECT,INSERT ON facts_projection_receipts TO cortex_test_runtime`); err != nil {
			t.Fatal(err)
		}
		// Dedicated session runtime parameter applies to every pooled connection.
		runtime, err := sql.Open("pgx", dsn+"&options=-c%20role%3Dcortex_test_runtime")
		if err != nil {
			t.Fatal(err)
		}
		runtime.SetMaxOpenConns(4)
		t.Cleanup(func() { runtime.Close() })
		index, err := cortex.New(ctx, runtime, c, embedder{})
		if err != nil {
			t.Fatal(err)
		}
		wrong := c
		wrong.Tenant = "different"
		if _, err := cortex.New(ctx, runtime, wrong, embedder{}); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("cross-tenant binding", err)
		}
		wrong = c
		wrong.Generation = "g2"
		if _, err := cortex.New(ctx, runtime, wrong, embedder{}); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("wrong generation binding", err)
		}
		return index, db
	}
	a, admin := setup(dsn, "bank-a")
	b, _ := setup(dsn2, "bank-b")
	text := "canonical accepted fact"
	d := model.ProjectionDocument{Tenant: "bank-a", Generation: "g1", RecordID: "record", VersionID: "version", VersionDigest: model.BytesDigest([]byte(text)), PolicyID: "policy", Classification: "confidential", Key: model.FactKey{Scope: "scope1", Subject: "subject", Predicate: "predicate"}, Value: model.Value{Kind: model.Text, Text: &text}}
	if err := a.Put(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := a.Put(ctx, d); err != nil {
		t.Fatal(err)
	}
	changed := d
	changed.PolicyID = "different"
	if err := a.Put(ctx, changed); !errors.Is(err, model.ErrConflict) {
		t.Fatal("changed immutable projection", err)
	}
	if err := b.Put(ctx, d); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross-tenant write", err)
	}
	hits, err := a.Candidates(ctx, "bank-a", "g1", "scope1", "fact", 10)
	if err != nil || len(hits) != 1 || hits[0].VersionID != "version" {
		t.Fatal(hits, err)
	}
	hits, err = a.Candidates(ctx, "bank-a", "g1", "scope2", "fact", 10)
	if err != nil || len(hits) != 0 {
		t.Fatal("scope leak", hits, err)
	}
	hits, err = b.Candidates(ctx, "bank-b", "g1", "scope1", "fact", 10)
	if err != nil || len(hits) != 0 {
		t.Fatal("database isolation", hits, err)
	}

	lexical := a.Lexical()
	for _, query := range []string{"canonical", "canonical fact"} {
		hits, err = lexical.Candidates(ctx, "bank-a", "g1", "scope1", query, 10)
		if err != nil || len(hits) != 1 || hits[0].VersionID != "version" {
			t.Fatal("lexical match", hits, err)
		}
	}
	hits, err = lexical.Candidates(ctx, "bank-a", "g1", "scope1", "unmatchedterm", 10)
	if err != nil || len(hits) != 0 {
		t.Fatal("unmatched lexical query", hits, err)
	}
	hits, err = lexical.Candidates(ctx, "bank-a", "g1", "scope2", "fact", 10)
	if err != nil || len(hits) != 0 {
		t.Fatal("lexical scope leak", hits, err)
	}
	if _, err = lexical.Candidates(ctx, "bank-b", "g1", "scope1", "fact", 10); !errors.Is(err, model.ErrDenied) {
		t.Fatal("lexical tenant leak", err)
	}
	identity, err := lexical.DescribeRetrieval(ctx, "bank-a", "g1")
	if err != nil || identity.EmbeddingID != "none" || identity.Dimensions != 0 {
		t.Fatal(identity, err)
	}
	var n int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM embeddings`).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Candidates(ctx, "bank-a", "g1", "scope1", "fact", 10); err != nil {
		t.Fatal("adapter unexpectedly closed host pool", err)
	}
}
