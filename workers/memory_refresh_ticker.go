package workers

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

// MemoryRefreshTicker emits durable sweep events. It starts no goroutines;
// Run is explicitly supervised by the host. A tick never calls a model.
type MemoryRefreshTicker struct {
	starter         model.MemoryRefreshSweepStarter
	caller          model.Caller
	stream, purpose string
	interval        time.Duration
	bindings        []model.MemoryRefreshBinding
}

func NewMemoryRefreshTicker(s model.MemoryRefreshSweepStarter, c model.Caller, stream, purpose string, bindings []model.MemoryRefreshBinding, interval time.Duration) (*MemoryRefreshTicker, error) {
	if s == nil || !c.Service || !model.ValidID(c.Tenant) || !model.ValidID(c.Principal) || !model.ValidID(stream) || len(purpose) == 0 || len(purpose) > 256 || interval < time.Minute || interval > 24*time.Hour || interval%time.Minute != 0 || len(bindings) < 1 || len(bindings) > 64 {
		return nil, model.ErrInvalid
	}
	seen := map[string]bool{}
	for _, binding := range bindings {
		if !model.ValidID(binding.Scope) || !model.ValidID(binding.ProfileID) || seen[binding.Scope] {
			return nil, model.ErrInvalid
		}
		seen[binding.Scope] = true
	}
	return &MemoryRefreshTicker{s, c, stream, purpose, interval, append([]model.MemoryRefreshBinding(nil), bindings...)}, nil
}
func (w *MemoryRefreshTicker) Tick(ctx context.Context, at time.Time) ([]model.CommitResult, error) {
	if at.IsZero() {
		return nil, model.ErrInvalid
	}
	bucket := at.UTC().Truncate(w.interval)
	results := []model.CommitResult{}
	for _, binding := range w.bindings {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		key := model.Digest([]string{"memory-refresh-sweep-v1", w.stream, binding.Scope, bucket.Format(time.RFC3339Nano)})
		result, err := w.starter.StartMemoryRefreshSweep(ctx, w.caller, model.MemoryRefreshSweepRequest{Command: model.Command{Key: key, Purpose: w.purpose}, Scope: binding.Scope, Stream: w.stream, ProfileID: binding.ProfileID, ScheduledFor: bucket, Interval: w.interval})
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}
func (w *MemoryRefreshTicker) Run(ctx context.Context) error {
	for {
		if _, err := w.Tick(ctx, time.Now()); err != nil {
			return err
		}
		next := time.Now().UTC().Truncate(w.interval).Add(w.interval)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
