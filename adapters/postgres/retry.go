package postgres

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// A failed attempt has rolled back and released its pool connection before this
// loop waits. Fresh attempts retain RepeatableRead isolation and the same callback.
func retryTransaction(ctx context.Context, transact func() error) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = transact()
		var pgError *pgconn.PgError
		if !errors.As(err, &pgError) || (pgError.Code != "40001" && pgError.Code != "40P01" && pgError.Code != "23505") {
			return translate(err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < 2 && pgError.Code != "23505" {
			if err := waitTransactionRetry(ctx, transactionRetryDelay(attempt)); err != nil {
				return err
			}
		}
	}
	return translate(err)
}

// Nominal waits are 1–5 ms before attempt two and 2–10 ms before attempt three.
// Jitter is scheduling noise, not a security primitive.
func transactionRetryDelay(attempt int) time.Duration {
	minimum := time.Millisecond * time.Duration(1<<attempt)
	return minimum + time.Duration(rand.Int64N(int64(4*minimum)+1))
}
func waitTransactionRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
