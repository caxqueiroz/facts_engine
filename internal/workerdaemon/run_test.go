package workerdaemon

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

type processorFunc func(context.Context) (bool, error)

func (f processorFunc) ProcessOne(ctx context.Context) (bool, error) { return f(ctx) }
func TestRunLoopBoundaries(t *testing.T) {
	t.Run("delivery timeout", func(t *testing.T) {
		calls := 0
		err := RunLoop(context.Background(), processorFunc(func(ctx context.Context) (bool, error) { calls++; <-ctx.Done(); return true, ctx.Err() }), 10*time.Millisecond, time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
			t.Fatalf("timeout: %v calls=%d", err, calls)
		}
	})
	t.Run("canceled delivery", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		err := RunLoop(ctx, processorFunc(func(ctx context.Context) (bool, error) { calls++; cancel(); <-ctx.Done(); return true, ctx.Err() }), time.Second, time.Millisecond)
		if err != nil || calls != 1 {
			t.Fatalf("shutdown: %v calls=%d", err, calls)
		}
	})
	t.Run("queue failure", func(t *testing.T) {
		expected := errors.New("queue failure")
		err := RunLoop(context.Background(), processorFunc(func(context.Context) (bool, error) { return false, expected }), time.Second, time.Millisecond)
		if !errors.Is(err, expected) {
			t.Fatal(err)
		}
	})
	t.Run("idle shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := RunLoop(ctx, processorFunc(func(context.Context) (bool, error) { cancel(); return false, nil }), time.Second, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
	})
}

type statusQueue struct {
	model.Outbox
	state string
}

func (q statusQueue) ConsumerStatus(context.Context, model.ConsumerKey) (model.ConsumerStatus, error) {
	return model.ConsumerStatus{State: q.state}, nil
}
func TestFailedConsumerDoesNotSilentlyPoll(t *testing.T) {
	for _, state := range []string{"failed", "ready", "paused", "retired"} {
		p := checkedProcessor{Processor: processorFunc(func(context.Context) (bool, error) { return false, nil }), queue: statusQueue{state: state}}
		_, err := p.ProcessOne(context.Background())
		if (err != nil) != (state == "failed") {
			t.Fatalf("%s: %v", state, err)
		}
	}
}
