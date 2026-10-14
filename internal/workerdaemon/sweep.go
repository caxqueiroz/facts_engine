package workerdaemon

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

type SweepTicker interface {
	Tick(context.Context, time.Time) ([]model.CommitResult, error)
}

// RunSweepLoop emits the current UTC bucket immediately and then at interval
// boundaries. Each emission is bounded; outages do not replay historical ticks.
func RunSweepLoop(ctx context.Context, ticker SweepTicker, interval, timeout time.Duration) error {
	if ticker == nil || interval < time.Minute || interval > 24*time.Hour || interval%time.Minute != 0 || timeout <= 0 {
		return model.ErrInvalid
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		tickCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err := ticker.Tick(tickCtx, time.Now())
		if err == nil {
			err = tickCtx.Err()
		}
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		next := time.Now().UTC().Truncate(interval).Add(interval)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
