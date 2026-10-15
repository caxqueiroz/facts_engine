package workers

import (
	"context"
	"errors"

	"github.com/caxqueiroz/facts_engine/model"
)

type OrphanCleanupBinding struct {
	Scope        string `json:"scope"`
	PolicyID     string `json:"policy_id"`
	Jurisdiction string `json:"jurisdiction"`
}
type OrphanCleanupConfig struct {
	Tenant          string                 `json:"tenant"`
	Stream          string                 `json:"stream"`
	Purpose         string                 `json:"purpose"`
	Reason          string                 `json:"reason"`
	Bindings        []OrphanCleanupBinding `json:"bindings"`
	PageSize        int                    `json:"page_size"`
	MaxPages        int                    `json:"max_pages"`
	IntervalMinutes int                    `json:"interval_minutes"`
}

func (c *OrphanCleanupConfig) Validate() error {
	if c == nil || !model.ValidID(c.Tenant) || !model.ValidID(c.Stream) || c.Purpose == "" || len(c.Purpose) > 256 || c.Reason == "" || len(c.Reason) > 1024 || len(c.Bindings) < 1 || len(c.Bindings) > 64 || c.PageSize < 1 || c.PageSize > 100 || c.MaxPages < 1 || c.MaxPages > 100 || c.IntervalMinutes < 1 || c.IntervalMinutes > 1440 {
		return model.ErrInvalid
	}
	seen := map[OrphanCleanupBinding]bool{}
	for _, b := range c.Bindings {
		if !model.ValidID(b.Scope) || !model.ValidID(b.PolicyID) || !model.ValidID(b.Jurisdiction) || seen[b] {
			return model.ErrInvalid
		}
		seen[b] = true
	}
	return nil
}

type OrphanCleanupResult struct{ Scanned, Disposed, Skipped int }
type OrphanCleanup struct {
	engine model.OrphanEvidenceEngine
	caller model.Caller
	config OrphanCleanupConfig
}

func NewOrphanCleanup(e model.OrphanEvidenceEngine, c model.Caller, config OrphanCleanupConfig) (*OrphanCleanup, error) {
	if e == nil || !c.Service || !model.ValidID(c.Principal) || c.Tenant != config.Tenant || config.Validate() != nil {
		return nil, model.ErrInvalid
	}
	config.Bindings = append([]OrphanCleanupBinding(nil), config.Bindings...)
	return &OrphanCleanup{e, c, config}, nil
}

// Tick is bounded by pages per binding and the caller's deadline. A process
// restart scans again; stable object-specific command keys preserve receipts.
func (w *OrphanCleanup) Tick(ctx context.Context) (OrphanCleanupResult, error) {
	var result OrphanCleanupResult
	for _, binding := range w.config.Bindings {
		q := model.OrphanEvidenceQuery{Scope: binding.Scope, PolicyID: binding.PolicyID, Jurisdiction: binding.Jurisdiction, Purpose: w.config.Purpose, Limit: w.config.PageSize}
		for page := 0; page < w.config.MaxPages; page++ {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			objects, err := w.engine.ScanOrphanEvidence(ctx, w.caller, q)
			if err != nil {
				return result, err
			}
			if len(objects) > q.Limit {
				return result, model.ErrPrecondition
			}
			for _, object := range objects {
				if err := ctx.Err(); err != nil {
					return result, err
				}
				if object.Locator < q.AfterLocator || (object.Locator == q.AfterLocator && object.Version <= q.AfterVersion) {
					return result, model.ErrPrecondition
				}
				result.Scanned++
				key := model.Digest([]string{"orphan-cleanup-v1", w.config.Stream, binding.Scope, binding.PolicyID, object.Locator, object.Version})
				_, err := w.engine.DisposeOrphanEvidence(ctx, w.caller, model.OrphanEvidenceDisposalRequest{Command: model.Command{Key: key, Purpose: w.config.Purpose}, Scope: binding.Scope, PolicyID: binding.PolicyID, Jurisdiction: binding.Jurisdiction, Reason: w.config.Reason, Object: object})
				if errors.Is(err, model.ErrPrecondition) {
					result.Skipped++
				} else if err != nil {
					return result, err
				} else {
					result.Disposed++
				}
				q.AfterLocator = object.Locator
				q.AfterVersion = object.Version
			}
			if len(objects) < q.Limit {
				break
			}
		}
	}
	return result, ctx.Err()
}
