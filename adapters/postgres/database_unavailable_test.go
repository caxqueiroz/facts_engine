package postgres_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testDatabaseUnavailable(t *testing.T, ctx context.Context, cfg *pgxpool.Config, admin *pgxpool.Pool, cipher postgres.Cipher) {
	t.Helper()
	config := cfg.Copy()
	config.MaxConns = 1
	var refuse atomic.Bool
	config.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if refuse.Load() {
			return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
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
	var pid int32
	if err := pool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = ledger.Transact(ctx, "database-outage", func(tx model.Tx) error {
		calls++
		// Terminate only the dedicated test pool's single backend, never another
		// caller's connection or the database server itself.
		var terminated bool
		if err := admin.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated); err != nil || !terminated {
			t.Fatal("terminate dedicated backend", err)
		}
		_, err := tx.Revision(ctx)
		return err
	})
	if !errors.Is(err, model.ErrUnavailable) || calls != 1 {
		t.Fatal("database termination classification/retry", calls, err)
	}
	refuse.Store(true)
	pool.Reset()

	if body, err := store.Read(ctx, "database-outage", model.ObjectRef{Locator: "source", Version: "v1"}); err != model.ErrUnavailable || len(body) != 0 {
		t.Fatal("evidence outage became missing/corrupt content", body, err)
	}
	if _, err := store.Put(ctx, "database-outage", "source", []byte("private evidence")); err != model.ErrUnavailable {
		t.Fatal("evidence write outage", err)
	}
	caller := model.Caller{Tenant: "database-outage", Principal: "reader"}
	at := time.Now().UTC()
	query := model.FactQuery{Key: model.FactKey{Scope: "case", Subject: "subject", Predicate: "preference"}, ValidAt: at, Purpose: "review"}
	result, err := engine.GetFact(ctx, caller, query)
	if !errors.Is(err, model.ErrUnavailable) || result.VersionID != "" || result.Value != nil {
		t.Fatal("embedded outage", result, err)
	}
	client := v1.NewFactServiceClient(governanceGRPC(t, engine, caller))
	remote, err := client.GetFact(ctx, &v1.GetFactRequest{Tenant: caller.Tenant, Key: &v1.FactKey{Scope: query.Key.Scope, Subject: query.Key.Subject, Predicate: query.Key.Predicate}, ValidAt: timestamppb.New(at), Purpose: query.Purpose})
	if remote != nil || status.Code(err) != codes.Unavailable || status.Convert(err).Message() != model.ErrUnavailable.Error() {
		t.Fatal("gRPC outage classification/disclosure", remote, err)
	}
	refuse.Store(false)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal("test connection did not recover", err)
	}
}
