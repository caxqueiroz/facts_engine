package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func benchmarkInt(b *testing.B, key string, fallback, min, max int) int {
	b.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < min || value > max {
		b.Fatalf("%s must be between %d and %d", key, min, max)
	}
	return value
}

// BenchmarkLedgerWorkload measures the public scope APIs, including authorization,
// encrypted events, command receipts, transaction retries and atomic outbox writes.
// It intentionally does not claim to measure just SQL lock acquisition time.
func BenchmarkLedgerWorkload(b *testing.B) {
	dsn := os.Getenv("TEST_BENCH_DATABASE_URL")
	if dsn == "" {
		b.Skip("set TEST_BENCH_DATABASE_URL to an initialized disposable integration database")
	}
	tenants := benchmarkInt(b, "BENCH_TENANTS", 1, 1, 1024)
	concurrency := benchmarkInt(b, "BENCH_CONCURRENCY", 8, 1, 256)
	connections := benchmarkInt(b, "BENCH_CONNECTIONS", 8, 1, 64)
	readPercent := benchmarkInt(b, "BENCH_READ_PERCENT", 0, 0, 100)
	corpus := benchmarkInt(b, "BENCH_SCOPES_PER_TENANT", 1, 1, 10000)
	b.Run(fmt.Sprintf("tenants=%d/workers=%d/connections=%d/read_percent=%d/scopes=%d", tenants, concurrency, connections, readPercent, corpus), func(b *testing.B) {
		b.StopTimer()
		ctx := context.Background()
		admin, err := pgxpool.New(ctx, dsn)
		if err != nil {
			b.Fatal(err)
		}
		defer admin.Close()
		config, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			b.Fatal(err)
		}
		config.MaxConns = int32(connections)
		config.ConnConfig.RuntimeParams["role"] = "facts_runtime"
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			b.Fatal(err)
		}
		defer pool.Close()
		cipher, err := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
		if err != nil {
			b.Fatal(err)
		}
		ledger, err := postgres.New(ctx, pool, cipher)
		if err != nil {
			b.Fatal(err)
		}
		store, err := evidence.NewPostgres(pool, cipher)
		if err != nil {
			b.Fatal(err)
		}
		engine, err := facts.New(ledger, allow{}, store)
		if err != nil {
			b.Fatal(err)
		}
		callers := make([]model.Caller, tenants)
		prefix := "benchmark-" + uuid.NewString()
		request := func(id string) model.ScopeRequest {
			return model.ScopeRequest{Command: model.Command{Key: id, Purpose: "append_benchmark"}, Definition: model.ScopeDefinition{ID: id, Label: "benchmark scope"}, Reason: "benchmark"}
		}
		for i := range callers {
			callers[i] = model.Caller{Tenant: fmt.Sprintf("%s-%d", prefix, i), Principal: "benchmark"}
			for n := 0; n < corpus; n++ {
				if _, err := engine.PutScope(ctx, callers[i], request(fmt.Sprintf("seed-%d", n))); err != nil {
					b.Fatal(err)
				}
			}
		}
		var next, reads, writes, conflicts, failures atomic.Int64
		committed := make([]atomic.Int64, tenants)
		var histogram [64]atomic.Int64
		var ready, done sync.WaitGroup
		ready.Add(concurrency)
		done.Add(concurrency)
		start := make(chan struct{})
		for worker := 0; worker < concurrency; worker++ {
			go func() {
				defer done.Done()
				ready.Done()
				<-start
				for {
					id := next.Add(1) - 1
					if id >= int64(b.N) {
						return
					}
					// Mix the operation sequence so tenants and read/write choices are not
					// coupled by common factors in their configured counts.
					choice := uint64(id) + 0x9e3779b97f4a7c15
					choice = (choice ^ (choice >> 30)) * 0xbf58476d1ce4e5b9
					choice = (choice ^ (choice >> 27)) * 0x94d049bb133111eb
					choice ^= choice >> 31
					tenant := int(choice % uint64(tenants))
					read := int((choice>>32)%100) < readPercent
					callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					began := time.Now()
					var err error
					if read {
						_, err = engine.GetScope(callCtx, callers[tenant], model.IdentityQuery{Scope: fmt.Sprintf("seed-%d", (choice>>16)%uint64(corpus)), Purpose: "append_benchmark"})
					} else {
						_, err = engine.PutScope(callCtx, callers[tenant], request(fmt.Sprintf("write-%d", id)))
					}
					elapsed := time.Since(began)
					cancel()
					histogram[bits.Len64(uint64(elapsed.Nanoseconds()))].Add(1)
					switch {
					case err == nil:
						if read {
							reads.Add(1)
						} else {
							writes.Add(1)
							committed[tenant].Add(1)
						}
					case errors.Is(err, model.ErrRevision), errors.Is(err, model.ErrConflict):
						conflicts.Add(1)
					default:
						failures.Add(1)
					}
				}
			}()
		}
		ready.Wait()
		b.ResetTimer()
		began := time.Now()
		b.StartTimer()
		close(start)
		done.Wait()
		b.StopTimer()
		duration := time.Since(began).Seconds()
		b.ReportMetric(float64(writes.Load())/duration, "commits/s")
		b.ReportMetric(float64(reads.Load())/duration, "reads/s")
		b.ReportMetric(float64(conflicts.Load())/float64(b.N), "conflicts/op")
		b.ReportMetric(float64(failures.Load())/float64(b.N), "failures/op")
		for _, percentile := range []int64{50, 95, 99} {
			threshold := (int64(b.N)*percentile + 99) / 100
			var seen int64
			for bucket := range histogram {
				seen += histogram[bucket].Load()
				if seen >= threshold {
					b.ReportMetric(float64(uint64(1)<<bucket)/1000, fmt.Sprintf("p%d-upper-us", percentile))
					break
				}
			}
		}
		if failures.Load() != 0 {
			b.Fatal("unexpected operation failures; results are not a valid capacity measurement")
		}
		if readPercent == 0 && writes.Load() == 0 {
			b.Fatal("no successful appends")
		}
		for i, caller := range callers {
			var count, head, outbox int64
			if err := admin.QueryRow(ctx, "SELECT count(*),coalesce(max(ledger_revision),0) FROM ledger_events WHERE tenant_id=$1", caller.Tenant).Scan(&count, &head); err != nil {
				b.Fatal(err)
			}
			if err := admin.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE tenant_id=$1", caller.Tenant).Scan(&outbox); err != nil {
				b.Fatal(err)
			}
			if count != int64(corpus)+committed[i].Load() || head != count || outbox != count {
				b.Fatal("committed ledger/outbox coverage mismatch")
			}
		}
	})
}
