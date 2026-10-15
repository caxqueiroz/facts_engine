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

type projectionCleanupTickFunc func(context.Context) (workers.ProjectionCleanupResult, error)

func (f projectionCleanupTickFunc) Tick(ctx context.Context) (workers.ProjectionCleanupResult, error) {
	return f(ctx)
}
func TestProjectionCleanupLoopDeadlineAndShutdown(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, failure := range []error{model.ErrUnavailable, model.ErrDenied} {
		calls := 0
		err := RunProjectionCleanupLoop(context.Background(), projectionCleanupTickFunc(func(ctx context.Context) (workers.ProjectionCleanupResult, error) {
			calls++
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing pass deadline")
			}
			return workers.ProjectionCleanupResult{}, failure
		}), time.Minute, time.Second, logger)
		if !errors.Is(err, failure) || calls != 1 {
			t.Fatal("loop hid failure", err, calls)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	err := RunProjectionCleanupLoop(ctx, projectionCleanupTickFunc(func(ctx context.Context) (workers.ProjectionCleanupResult, error) {
		cancel()
		<-ctx.Done()
		return workers.ProjectionCleanupResult{}, ctx.Err()
	}), time.Minute, time.Second, logger)
	if err != nil {
		t.Fatal("shutdown", err)
	}
	err = RunProjectionCleanupLoop(context.Background(), projectionCleanupTickFunc(func(ctx context.Context) (workers.ProjectionCleanupResult, error) {
		<-ctx.Done()
		return workers.ProjectionCleanupResult{}, nil
	}), time.Minute, time.Millisecond, logger)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("late success bypassed deadline", err)
	}
}

func TestProjectionCleanupConfiguration(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.ProjectionCleanup = nil },
		func(c *Config) { c.ProjectionCleanup.PageSize = 101 },
		func(c *Config) { c.ProjectionCleanup.MaxPages = 0 },
		func(c *Config) { c.ProjectionCleanup.IntervalMinutes = 0 },
		func(c *Config) { c.ProjectionCleanup.Destinations = nil },
		func(c *Config) { c.ProjectionCleanup.Destinations[0].Tenant = "other" },
		func(c *Config) { c.ProjectionCleanup.Destinations[0].Dimensions = 3 },
		func(c *Config) { c.ProjectionCleanup.Destinations[0].DatabaseEnv = "invalid-name" },
		func(c *Config) {
			c.ProjectionCleanup.Destinations = append(c.ProjectionCleanup.Destinations, c.ProjectionCleanup.Destinations[0])
		},
		func(c *Config) { c.Mode = "projection" },
		func(c *Config) { c.LeaseSeconds = 30 },
		func(c *Config) { c.Refresh = &RefreshConfig{} },
	} {
		c, err := LoadConfig("../../examples/memoryworker-projection-cleanup.json")
		if err != nil {
			t.Fatal(err)
		}
		change(&c)
		if c.Validate() == nil {
			t.Fatal("invalid projection cleanup configuration accepted")
		}
	}
}
