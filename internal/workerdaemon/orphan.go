package workerdaemon

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"log/slog"
	"time"
)

type OrphanTicker interface {
	Tick(context.Context) (workers.OrphanCleanupResult, error)
}

func RunOrphanLoop(ctx context.Context, ticker OrphanTicker, interval, timeout time.Duration, logger *slog.Logger) error {
	if ticker == nil || logger == nil || interval < time.Minute || interval > 24*time.Hour || timeout <= 0 {
		return model.ErrInvalid
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		tickCtx, cancel := context.WithTimeout(ctx, timeout)
		result, err := ticker.Tick(tickCtx)
		if err == nil {
			err = tickCtx.Err()
		}
		cancel()
		logger.Info("orphan cleanup pass", "scanned", result.Scanned, "disposed", result.Disposed, "skipped", result.Skipped, "failed", err != nil)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
