package workers

import (
	"context"
	"errors"
	"sync"

	"github.com/caxqueiroz/facts_engine/model"
)

type ProjectionCleanupConfig struct {
	Tenant          string `json:"tenant"`
	Stream          string `json:"stream"`
	Reason          string `json:"reason"`
	PageSize        int    `json:"page_size"`
	MaxPages        int    `json:"max_pages"`
	IntervalMinutes int    `json:"interval_minutes"`
}

func (c *ProjectionCleanupConfig) Validate() error {
	if c == nil || !model.ValidID(c.Tenant) || !model.ValidID(c.Stream) || c.Reason == "" || len(c.Reason) > 1024 || c.PageSize < 1 || c.PageSize > 100 || c.MaxPages < 1 || c.MaxPages > 100 || c.IntervalMinutes < 1 || c.IntervalMinutes > 1440 {
		return model.ErrInvalid
	}
	return nil
}

type ProjectionCleanupResult struct{ Scanned, Completed, Skipped int }
type ProjectionCleanup struct {
	scanner model.ProjectionCleanupScanner
	engine  model.ProjectionCleanupEngine
	caller  model.Caller
	config  ProjectionCleanupConfig
	after   model.ProjectionCleanupCursor
	mu      sync.Mutex
}

func NewProjectionCleanup(scanner model.ProjectionCleanupScanner, engine model.ProjectionCleanupEngine, caller model.Caller, config ProjectionCleanupConfig) (*ProjectionCleanup, error) {
	if scanner == nil || engine == nil || config.Validate() != nil || !caller.Service || !model.ValidID(caller.Principal) || caller.Tenant != config.Tenant {
		return nil, model.ErrInvalid
	}
	return &ProjectionCleanup{scanner: scanner, engine: engine, caller: caller, config: config}, nil
}

// Tick advances a bounded in-memory cursor, including past held/frozen targets,
// so a long-lived worker eventually visits later pages. At EOF it starts a new
// scan on the next tick. Restarts safely rescan with stable command identities.
func (w *ProjectionCleanup) Tick(ctx context.Context) (ProjectionCleanupResult, error) {
	var result ProjectionCleanupResult
	if !w.mu.TryLock() {
		return result, model.ErrUnavailable
	}
	defer w.mu.Unlock()
	for page := 0; page < w.config.MaxPages; page++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		requests, err := w.scanner.ScanProjectionCleanup(ctx, model.ProjectionCleanupQuery{Tenant: w.caller.Tenant, After: w.after, Limit: w.config.PageSize})
		if err != nil {
			return result, err
		}
		if len(requests) > w.config.PageSize {
			return result, model.ErrPrecondition
		}
		// Validate the entire page before any deletion; malformed ordering must not
		// silently skip a target or make a partial page appear to be complete.
		cursor := w.after
		for _, r := range requests {
			next := model.ProjectionCleanupCursor{Kind: r.Kind, VersionID: r.VersionID, Generation: r.Generation}
			if !next.Valid() || next.Compare(cursor) <= 0 || !model.ValidID(r.DisposalEventID) || !model.ValidID(r.Scope) || !model.ValidID(r.RecordID) || r.Purpose == "" || r.Key != "" || r.CorrelationID != "" || r.Reason != "" {
				return result, model.ErrPrecondition
			}
			cursor = next
		}
		for _, r := range requests {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			r.Key = model.Digest([]string{"projection-cleanup-v1", w.config.Stream, w.caller.Tenant, r.Kind, r.VersionID, r.Generation, r.DisposalEventID})
			r.Reason = w.config.Reason
			result.Scanned++
			_, err = w.engine.CleanupProjection(ctx, w.caller, r)
			if errors.Is(err, model.ErrPrecondition) {
				result.Skipped++
			} else if err != nil {
				return result, err
			} else {
				result.Completed++
			}
			w.after = model.ProjectionCleanupCursor{Kind: r.Kind, VersionID: r.VersionID, Generation: r.Generation}
		}
		if len(requests) < w.config.PageSize {
			w.after = model.ProjectionCleanupCursor{}
			break
		}
	}
	return result, ctx.Err()
}
