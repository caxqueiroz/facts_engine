package workers

import (
	"context"
	"encoding/json"
	"fmt"

	internaltelemetry "github.com/caxqueiroz/facts_engine/internal/telemetry"
	"github.com/caxqueiroz/facts_engine/model"
)

// EvidenceDisposalHandler performs post-commit deletion from an external
// evidence store. The outbox retries partial family deletion, so the store's
// exact-version deletion operation must be idempotent.
type EvidenceDisposalHandler struct {
	observer *internaltelemetry.Observer
	source   model.ArchiveSource
	store    model.EvidenceVersionDeleter
	tenant   string
}

func NewEvidenceDisposalHandler(source model.ArchiveSource, store model.EvidenceVersionDeleter, tenant string) (*EvidenceDisposalHandler, error) {
	return NewEvidenceDisposalHandlerWithTelemetry(source, store, tenant, model.TelemetryOptions{})
}

func NewEvidenceDisposalHandlerWithTelemetry(source model.ArchiveSource, store model.EvidenceVersionDeleter, tenant string, telemetry model.TelemetryOptions) (*EvidenceDisposalHandler, error) {
	if source == nil || store == nil || !model.ValidID(tenant) {
		return nil, model.ErrInvalid
	}
	observer, err := internaltelemetry.New(telemetry)
	if err != nil {
		return nil, err
	}
	return &EvidenceDisposalHandler{observer: observer, source: source, store: store, tenant: tenant}, nil
}

func (h *EvidenceDisposalHandler) Handle(ctx context.Context, d model.Delivery) (err error) {
	ctx, operation := h.observer.Start(ctx, "dispose.evidence")
	defer operation.End(&err)
	if d.Key.Tenant != h.tenant || d.Key.Consumer != model.EvidenceDisposalConsumer() || d.Event.LedgerRevision < 1 {
		return model.ErrDenied
	}
	if d.Event.Type != "dispose_evidence" {
		return nil
	}
	if !model.ValidID(d.Event.EntityID) {
		return model.ErrPrecondition
	}
	payload, err := h.source.ArchivePayload(ctx, d)
	if err != nil {
		return err
	}
	if payload.Event.ID != d.Event.ID || payload.Event.EntityID != d.Event.EntityID || payload.Event.Type != d.Event.Type || payload.Event.Scope != d.Event.Scope || payload.Event.LedgerRevision != d.Event.LedgerRevision || payload.Event.Caller.Tenant != h.tenant {
		return model.ErrPrecondition
	}
	var disposal *model.RetentionEvent
	for _, entry := range payload.Entries {
		if entry.Kind != "retention" || entry.ID != d.Event.EntityID {
			continue
		}
		var candidate model.RetentionEvent
		if err := json.Unmarshal(entry.Payload, &candidate); err != nil {
			return err
		}
		if candidate.ID != d.Event.EntityID || candidate.LedgerRevision != d.Event.LedgerRevision || candidate.Tenant != h.tenant || candidate.Scope != d.Event.Scope {
			return model.ErrPrecondition
		}
		disposal = &candidate
		break
	}
	if disposal == nil || disposal.Kind != "dispose_evidence" || disposal.Tenant != h.tenant || disposal.Scope != d.Event.Scope || disposal.Disposal == nil {
		return model.ErrPrecondition
	}
	ids := append([]string{disposal.Disposal.EvidenceID}, disposal.Disposal.DerivativeEvidenceIDs...)
	if len(ids) == 0 || len(ids) > 257 {
		return model.ErrPrecondition
	}
	selected := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !model.ValidID(id) || selected[id] {
			return model.ErrPrecondition
		}
		selected[id] = true
	}
	evidence := make(map[string]model.Evidence, len(ids))
	for _, entry := range payload.Entries {
		if entry.Kind != "evidence" {
			continue
		}
		var item model.Evidence
		if err := json.Unmarshal(entry.Payload, &item); err != nil {
			return err
		}
		if item.ID != entry.ID || item.Scope != disposal.Scope || !model.ValidID(item.ID) || item.Object.Locator == "" || item.Object.Version == "" {
			return model.ErrPrecondition
		}
		evidence[item.ID] = item
	}
	// Delete in request order (root, then derivatives) so retries replay the same sequence.
	objects := make([]model.ObjectRef, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		item, ok := evidence[id]
		if !ok {
			return model.ErrPrecondition
		}
		key := fmt.Sprintf("%s\x00%s", item.Object.Locator, item.Object.Version)
		if seen[key] {
			continue
		}
		seen[key] = true
		objects = append(objects, item.Object)
	}
	for _, object := range objects {
		if err := h.store.DeleteEvidenceVersion(ctx, h.tenant, object); err != nil {
			return err
		}
	}
	return nil
}

var _ Handler = (*EvidenceDisposalHandler)(nil)
