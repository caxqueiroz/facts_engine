package workerdaemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
)

type orphanTickFunc func(context.Context) (workers.OrphanCleanupResult, error)

func (f orphanTickFunc) Tick(ctx context.Context) (workers.OrphanCleanupResult, error) { return f(ctx) }
func TestOrphanLoopDeadlineAndShutdown(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, failure := range []error{model.ErrUnavailable, model.ErrDenied} {
		calls := 0
		err := RunOrphanLoop(context.Background(), orphanTickFunc(func(ctx context.Context) (workers.OrphanCleanupResult, error) {
			calls++
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing pass deadline")
			}
			return workers.OrphanCleanupResult{}, failure
		}), time.Minute, time.Second, logger)
		if !errors.Is(err, failure) || calls != 1 {
			t.Fatal("loop hid failure", err, calls)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	err := RunOrphanLoop(ctx, orphanTickFunc(func(ctx context.Context) (workers.OrphanCleanupResult, error) {
		cancel()
		<-ctx.Done()
		return workers.OrphanCleanupResult{}, ctx.Err()
	}), time.Minute, time.Second, logger)
	if err != nil {
		t.Fatal("shutdown", err)
	}
	err = RunOrphanLoop(context.Background(), orphanTickFunc(func(ctx context.Context) (workers.OrphanCleanupResult, error) {
		<-ctx.Done()
		return workers.OrphanCleanupResult{}, nil
	}), time.Minute, time.Millisecond, logger)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("late success bypassed deadline", err)
	}
}
func TestOrphanConfiguration(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.OrphanCleanup = nil },
		func(c *Config) { c.OrphanCleanup.PageSize = 101 },
		func(c *Config) { c.OrphanCleanup.MaxPages = 0 },
		func(c *Config) { c.OrphanCleanup.IntervalMinutes = 0 },
		func(c *Config) {
			c.OrphanCleanup.Bindings = append(c.OrphanCleanup.Bindings, c.OrphanCleanup.Bindings[0])
		},
		func(c *Config) { c.Mode = "projection" },
		func(c *Config) { c.LeaseSeconds = 30 },
		func(c *Config) { c.Refresh = &RefreshConfig{} },
	} {
		c, err := LoadConfig("../../examples/memoryworker-orphan-cleanup.json")
		if err != nil {
			t.Fatal(err)
		}
		change(&c)
		if c.Validate() == nil {
			t.Fatal("invalid cleanup config accepted", c)
		}
	}
}
