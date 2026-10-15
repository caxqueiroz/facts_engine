// Package pgcleanup bounds PostgreSQL transaction cleanup independently of requests.
package pgcleanup

import (
	"context"
	"time"
)

// Rollback gives cleanup a fresh deadline even when the request has expired.
// The driver discards connections when rollback fails. Cleanup errors must not
// replace the operation's original error (or a successful commit).
func Rollback(ctx context.Context, rollback func(context.Context) error) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = rollback(cleanup)
}
