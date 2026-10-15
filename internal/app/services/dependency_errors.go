package services

import (
	"context"
	"errors"

	"github.com/caxqueiroz/facts_engine/model"
)

// unavailableDependency sanitizes dependency details while preserving request
// interruption, including adapters that return a wrapped interruption while
// the parent context is still active. Call after I/O even when err is nil.
func unavailableDependency(ctx context.Context, err error) error {
	if interrupted := ctx.Err(); interrupted != nil {
		return interrupted
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if err != nil {
		return model.ErrUnavailable
	}
	return nil
}
