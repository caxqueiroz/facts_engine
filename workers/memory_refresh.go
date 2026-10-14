package workers

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
)

// MemoryRefresh schedules durable jobs; it never invokes a model. Use a separate
// extraction consumer to execute them, with independently visible progress.
type MemoryRefresh struct {
	scheduler       model.MemoryRefreshScheduler
	caller          model.Caller
	stream, purpose string
	bindings        []model.MemoryRefreshBinding
}

func NewMemoryRefresh(s model.MemoryRefreshScheduler, c model.Caller, stream, purpose string, bindings []model.MemoryRefreshBinding) (*MemoryRefresh, error) {
	if s == nil || !c.Service || !model.ValidID(c.Tenant) || !model.ValidID(c.Principal) || !model.ValidID(stream) || len(purpose) == 0 || len(purpose) > 256 || len(bindings) < 1 || len(bindings) > 64 {
		return nil, model.ErrInvalid
	}
	seen := map[string]bool{}
	for _, binding := range bindings {
		if !model.ValidID(binding.Scope) || !model.ValidID(binding.ProfileID) || seen[binding.Scope] {
			return nil, model.ErrInvalid
		}
		seen[binding.Scope] = true
	}
	return &MemoryRefresh{s, c, stream, purpose, append([]model.MemoryRefreshBinding(nil), bindings...)}, nil
}
func (w *MemoryRefresh) Handle(ctx context.Context, d model.Delivery) error {
	if d.Key.Tenant != w.caller.Tenant || d.Key.Consumer != model.MemoryRefreshConsumer(w.stream) || d.Event.Caller.Tenant != w.caller.Tenant {
		return model.ErrDenied
	}
	sweep := model.IsMemoryRefreshSweep(d.Event)
	if sweep && d.Event.RefreshSweep.Stream != w.stream {
		return nil
	}
	if !model.ChangesMemoryDependency(d.Event) && !sweep {
		return nil
	}
	if !model.ValidID(d.Event.ID) || d.Event.LedgerRevision < 1 || d.Fence < 1 {
		return model.ErrInvalid
	}
	trigger := model.MemoryRefreshTrigger{EventID: d.Event.ID, LedgerRevision: d.Event.LedgerRevision}
	for _, binding := range w.bindings {
		if sweep {
			if d.Event.Scope != binding.Scope {
				continue
			}
			if d.Event.RefreshSweep.ProfileID != binding.ProfileID || d.Event.Command.Purpose != w.purpose {
				return model.ErrPrecondition
			}
		}
		after := ""
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			page, err := w.scheduler.ScanMemoryRefresh(ctx, w.caller, model.MemoryRefreshScan{Scope: binding.Scope, Purpose: w.purpose, Trigger: trigger, AfterVersionID: after, Limit: 100})
			if err != nil {
				return err
			}
			if len(page.Versions) > 100 {
				return model.ErrPrecondition
			}
			for _, version := range page.Versions {
				if !model.ValidID(version.RecordID) || !model.ValidID(version.VersionID) || version.VersionID <= after {
					return model.ErrPrecondition
				}
				command := model.Command{Key: model.Digest([]string{d.Key.Consumer, d.Event.ID, binding.Scope, version.VersionID}), Purpose: w.purpose, CorrelationID: d.Event.Command.CorrelationID}
				_, err = w.scheduler.StartMemoryRefresh(ctx, w.caller, model.MemoryRefreshRequest{Command: command, Scope: binding.Scope, RecordID: version.RecordID, VersionID: version.VersionID, ProfileID: binding.ProfileID, Trigger: &trigger})
				if err != nil && !errors.Is(err, model.ErrRefreshNotNeeded) {
					return err
				}
				after = version.VersionID
			}
			if len(page.Versions) < 100 {
				break
			}
		}
	}
	return nil
}
