package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

type memoryRefreshSweepCommand struct {
	model.MemoryRefreshSweepRequest
	ProfileDigest string
}

func (s *Service) StartMemoryRefreshSweep(ctx context.Context, c model.Caller, r model.MemoryRefreshSweepRequest) (model.CommitResult, error) {
	if !model.ValidID(r.Stream) || !model.ValidID(r.ProfileID) || r.Interval < time.Minute || r.Interval > 24*time.Hour || r.Interval%time.Minute != 0 || r.ScheduledFor.IsZero() || r.ScheduledFor.After(time.Now().Add(time.Second)) {
		return model.CommitResult{}, model.ErrInvalid
	}
	r.ScheduledFor = r.ScheduledFor.UTC()
	if !r.ScheduledFor.Equal(r.ScheduledFor.Truncate(r.Interval)) {
		return model.CommitResult{}, model.ErrInvalid
	}
	approved, ok := s.extractors[r.ProfileID]
	if !ok || approved.Profile.ParserVersion != "memory-refresh-json-v1" {
		return model.CommitResult{}, model.ErrPrecondition
	}
	for _, permission := range []string{"can_read", "can_read_history"} {
		if _, err := s.authorize(ctx, c, r.Scope, permission, r.Command); err != nil {
			return model.CommitResult{}, err
		}
	}
	input := memoryRefreshSweepCommand{r, model.ExtractionProfileDigest(approved.Profile)}
	return s.mutate(ctx, c, r.Scope, "sweep_memory_refresh", "can_extract", r.Command, input, func(_ model.Tx, out *model.CommitResult) error {
		out.ContentDigest = input.ProfileDigest
		return nil
	})
}
