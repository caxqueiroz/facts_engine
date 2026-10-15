package pgcleanup

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRollbackHasIndependentBoundedLifetime(t *testing.T) {
	type key struct{}
	parent, cancel := context.WithDeadline(context.WithValue(context.Background(), key{}, "request"), time.Now().Add(-time.Second))
	defer cancel()
	var retained context.Context
	start := time.Now()
	Rollback(parent, func(ctx context.Context) error {
		retained = ctx
		if ctx.Err() != nil || ctx.Value(key{}) != "request" {
			t.Fatal("cleanup must survive request expiry and retain context values")
		}
		deadline, ok := ctx.Deadline()
		if !ok || deadline.Before(start.Add(5*time.Second)) || deadline.After(time.Now().Add(5*time.Second)) {
			t.Fatal("cleanup must have a fresh five-second deadline")
		}
		return errors.New("rollback failed")
	})
	if retained == nil || !errors.Is(retained.Err(), context.Canceled) {
		t.Fatal("cleanup context must be released after rollback")
	}
}
