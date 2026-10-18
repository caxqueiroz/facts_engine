package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"sort"
	"strings"
	"testing"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaOwnerTestRole = "facts_schema_owner_test"

// TestLedgerInNamedSchema installs the ledger into a "facts" schema of its own
// database, so it never touches the database TestPostgresLedger requires fresh.
func TestLedgerInNamedSchema(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to a disposable PostgreSQL database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanups run last-in first-out: the database is dropped before admin closes.
	t.Cleanup(admin.Close)
	var current string
	if err = admin.QueryRow(ctx, "SELECT current_database()").Scan(&current); err != nil {
		t.Fatal(err)
	}
	database := current + "_schema"
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	var pools []*pgxpool.Pool
	t.Cleanup(func() {
		for _, p := range pools {
			p.Close()
		}
		if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop schema test database: %v", err)
		}
		if _, err := admin.Exec(ctx, "DROP ROLE IF EXISTS "+schemaOwnerTestRole); err != nil {
			t.Errorf("drop schema owner test role: %v", err)
		}
	})
	connect := func(role, searchPath string) *pgxpool.Pool {
		t.Helper()
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.Database = database
		if role != "" {
			cfg.ConnConfig.RuntimeParams["role"] = role
		}
		cfg.ConnConfig.RuntimeParams["search_path"] = searchPath
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		pools = append(pools, p)
		return p
	}

	setup := connect("", "public")
	if _, err = setup.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS btree_gist SCHEMA public; CREATE SCHEMA facts"); err != nil {
		t.Fatal(err)
	}
	owner := connect("", "facts")
	migrations := facts.Migrations()
	names, err := fs.Glob(migrations, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	for _, name := range names {
		b, err := fs.ReadFile(migrations, name)
		if err != nil {
			t.Fatal(err)
		}
		up := strings.Split(string(b), "-- +goose Down")[0]
		if _, err = owner.Exec(ctx, up); err != nil {
			t.Fatalf("migration %s: %v", name, err)
		}
	}
	role, err := os.ReadFile("../../scripts/runtime-role.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = owner.Exec(ctx, string(role)); err != nil {
		t.Fatalf("runtime role script: %v", err)
	}
	var publicTables int
	if err = owner.QueryRow(ctx, "SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname='public'").Scan(&publicTables); err != nil {
		t.Fatal(err)
	}
	if publicTables != 0 {
		t.Fatalf("migrations created %d tables in public", publicTables)
	}

	// The table owner is a separate role so the superuser connection can SET ROLE to it.
	if _, err = owner.Exec(ctx, "DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='"+schemaOwnerTestRole+"') THEN CREATE ROLE "+schemaOwnerTestRole+" NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$"); err != nil {
		t.Fatal(err)
	}
	if _, err = owner.Exec(ctx, "GRANT USAGE ON SCHEMA facts TO "+schemaOwnerTestRole+"; ALTER TABLE facts.ledger_events OWNER TO "+schemaOwnerTestRole); err != nil {
		t.Fatal(err)
	}

	cipher, err := postgres.NewAESGCM(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	runtime := connect("facts_runtime", "facts")
	for _, c := range []struct {
		name    string
		pool    *pgxpool.Pool
		opts    []postgres.Option
		want    error
		message string
	}{
		{name: "runtime role in named schema", pool: runtime, opts: []postgres.Option{postgres.WithSchema("facts")}},
		{name: "default schema refused on facts search path", pool: runtime, want: model.ErrPrecondition, message: "search_path must resolve"},
		{name: "public search path refused for facts schema", pool: connect("facts_runtime", "public"), opts: []postgres.Option{postgres.WithSchema("facts")}, want: model.ErrPrecondition, message: "search_path must resolve"},
		{name: "table owner refused", pool: connect(schemaOwnerTestRole, "facts"), opts: []postgres.Option{postgres.WithSchema("facts")}, want: model.ErrPrecondition, message: "must not own ledger tables"},
		{name: "invalid schema name", pool: runtime, opts: []postgres.Option{postgres.WithSchema("Bad-Name")}, want: model.ErrInvalid},
	} {
		t.Run(c.name, func(t *testing.T) {
			ledger, err := postgres.New(ctx, c.pool, cipher, c.opts...)
			if c.want == nil {
				if err != nil || ledger == nil {
					t.Fatalf("New = %v, %v", ledger, err)
				}
				// Unqualified runtime queries resolve to the named schema.
				if err = ledger.Transact(ctx, "tenant-schema", func(tx model.Tx) error {
					_, err := tx.Revision(ctx)
					return err
				}); err != nil {
					t.Fatalf("Transact in named schema: %v", err)
				}
				return
			}
			if !errors.Is(err, c.want) || !strings.Contains(err.Error(), c.message) {
				t.Fatalf("New error = %v, want %v containing %q", err, c.want, c.message)
			}
		})
	}

	t.Run("projection cleanup acknowledgement privilege", func(t *testing.T) {
		for _, c := range []struct {
			role string
			want bool
		}{{"facts_runtime", false}, {"facts_retention", true}} {
			got, err := ledgerdb.New(connect(c.role, "facts")).CanAcknowledgeProjectionCleanup(ctx, "facts")
			if err != nil || got != c.want {
				t.Fatalf("%s CanAcknowledgeProjectionCleanup = %v, %v; want %v", c.role, got, err, c.want)
			}
		}
	})
}
