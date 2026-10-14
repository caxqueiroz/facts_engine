package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestTransactionRetryOutcomes(t *testing.T) {
	for _, test := range []struct {
		code     string
		attempts int
		want     error
	}{
		{"40001", 3, model.ErrRevision}, {"40P01", 3, model.ErrRevision}, {"23505", 3, model.ErrConflict},
		{"23503", 1, model.ErrPrecondition}, {"42501", 1, model.ErrDenied},
	} {
		t.Run(test.code, func(t *testing.T) {
			calls := 0
			err := retryTransaction(context.Background(), func() error { calls++; return fmt.Errorf("statement: %w", &pgconn.PgError{Code: test.code}) })
			if !errors.Is(err, test.want) || calls != test.attempts {
				t.Fatalf("calls=%d, error=%v", calls, err)
			}
		})
	}
	for _, code := range []string{"40001", "40P01", "23505"} {
		calls := 0
		err := retryTransaction(context.Background(), func() error {
			calls++
			if calls == 1 {
				return &pgconn.PgError{Code: code}
			}
			return nil
		})
		if err != nil || calls != 2 {
			t.Fatalf("retry did not recover: %s calls=%d, error=%v", code, calls, err)
		}
	}
	sentinel := errors.New("application failure")
	calls := 0
	err := retryTransaction(context.Background(), func() error { calls++; return sentinel })
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatal("application failure retried")
	}
}
func TestTransactionRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	if err := retryTransaction(ctx, func() error { calls++; return nil }); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatal("canceled request executed")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	calls = 0
	err := retryTransaction(ctx, func() error { calls++; cancel(); return &pgconn.PgError{Code: "40001"} })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal("cancellation did not stop retry")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := waitTransactionRetry(ctx, time.Hour); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("backoff ignored deadline", err)
	}
}
func TestTransactionRetryDelayBounds(t *testing.T) {
	for attempt := 0; attempt < 2; attempt++ {
		minimum := time.Millisecond * time.Duration(1<<attempt)
		for n := 0; n < 100; n++ {
			delay := transactionRetryDelay(attempt)
			if delay < minimum || delay > 5*minimum {
				t.Fatalf("unbounded retry delay: %v", delay)
			}
		}
	}
}
