package workerdaemon

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

type sweepFunc func(context.Context, time.Time) ([]model.CommitResult, error)

func (f sweepFunc) Tick(ctx context.Context, at time.Time) ([]model.CommitResult, error) {
	return f(ctx, at)
}
func TestSweepLoopBoundaries(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		calls := 0
		err := RunSweepLoop(context.Background(), sweepFunc(func(ctx context.Context, at time.Time) ([]model.CommitResult, error) {
			calls++
			if at.IsZero() {
				t.Error("missing tick time")
			}
			<-ctx.Done()
			return nil, nil
		}), time.Minute, 10*time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
			t.Fatal(err, calls)
		}
	})
	t.Run("failure", func(t *testing.T) {
		err := RunSweepLoop(context.Background(), sweepFunc(func(context.Context, time.Time) ([]model.CommitResult, error) { return nil, model.ErrDenied }), time.Minute, time.Second)
		if !errors.Is(err, model.ErrDenied) {
			t.Fatal(err)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := RunSweepLoop(ctx, sweepFunc(func(ctx context.Context, _ time.Time) ([]model.CommitResult, error) {
			cancel()
			<-ctx.Done()
			return nil, ctx.Err()
		}), time.Minute, time.Second)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("idle shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ticked := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- RunSweepLoop(ctx, sweepFunc(func(context.Context, time.Time) ([]model.CommitResult, error) { close(ticked); return nil, nil }), time.Hour, time.Second)
		}()
		<-ticked
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("sweep shutdown hung")
		}
	})
	for _, interval := range []time.Duration{0, time.Second, time.Minute + time.Second, 25 * time.Hour} {
		if err := RunSweepLoop(context.Background(), sweepFunc(func(context.Context, time.Time) ([]model.CommitResult, error) {
			t.Fatal("invalid interval emitted sweep")
			return nil, nil
		}), interval, time.Second); !errors.Is(err, model.ErrInvalid) {
			t.Fatal(err)
		}
	}
}
