package workers

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

// Rebuild coordinates durable ledger states and fenced destination gates.
// Hosts authorize operators and provision destination databases separately.
type Rebuild struct {
	store model.RebuildStore
	index model.RebuildIndex
	actor model.RebuildActor
}

func NewRebuild(store model.RebuildStore, index model.RebuildIndex, actor model.RebuildActor) (*Rebuild, error) {
	if store == nil || index == nil || !model.ValidID(actor.Principal) || actor.Purpose == "" {
		return nil, model.ErrInvalid
	}
	return &Rebuild{store, index, actor}, nil
}
func (r *Rebuild) Start(ctx context.Context, k model.GenerationKey, attempts int) (model.RebuildStatus, error) {
	s, err := r.store.StartRebuild(ctx, k, r.actor, attempts)
	if err != nil {
		return s, err
	}
	if s.State == "building" {
		err = r.index.SetProjectionGate(ctx, k, s.Fence, false)
	}
	return s, err
}
func (r *Rebuild) Verify(ctx context.Context, k model.GenerationKey) (model.RebuildStatus, error) {
	s, err := r.store.FreezeRebuild(ctx, k, r.actor)
	if err != nil {
		return s, err
	}
	if err = r.index.SetProjectionGate(ctx, k, s.Fence, true); err != nil {
		return s, err
	}
	var check model.ProjectionCheck
	pages, sourcePaged := r.store.(model.PagedRebuildStore)
	verifier, indexPaged := r.index.(model.PagedRebuildIndex)
	// Already verified legacy generations keep their original digest contract.
	// Resume/freeze upgrades them to paged verification without changing a proof.
	legacyProof := s.State == "verified" && s.VerificationVersion == ""
	if sourcePaged && indexPaged && !legacyProof {
		check, err = verifier.VerifyProjectionPages(ctx, k, s.Fence, func(ctx context.Context, after model.ProjectionCursor) (model.ProjectionPage, error) {
			return pages.ProjectionDocumentPage(ctx, k, s.Fence, s.ThroughRevision, after, 100)
		})
		if err == nil && check.Version != model.ProjectionInventoryV2 {
			return s, model.ErrPrecondition
		}
	} else {
		if s.VerificationVersion != "" {
			return s, model.ErrPrecondition
		}
		var docs []model.ProjectionDocument
		docs, err = r.store.ProjectionDocuments(ctx, k, s.ThroughRevision)
		if err == nil {
			check, err = r.index.VerifyProjection(ctx, k, s.Fence, docs)
		}
	}
	if err != nil {
		return s, err
	}

	return r.store.VerifyRebuild(ctx, k, r.actor, s.Fence, check)
}
func (r *Rebuild) Resume(ctx context.Context, k model.GenerationKey, fence int64) (model.RebuildStatus, error) {
	s, err := r.store.ResumeRebuild(ctx, k, r.actor, fence)
	if err != nil {
		return s, err
	}
	if err = r.index.SetProjectionGate(ctx, k, s.Fence, false); err != nil {
		return s, err
	}
	return r.store.FinishResume(ctx, k, r.actor, s.Fence)
}
func (r *Rebuild) Activate(ctx context.Context, k model.GenerationKey, verifiedFence int64) (model.RebuildStatus, error) {
	s, err := r.store.ActivateRebuild(ctx, k, r.actor, verifiedFence)
	if err != nil {
		return s, err
	}
	// Idempotent cleanup after the committed switch. If it fails, repeating
	// Activate resumes it; managed reads already bind only to the verified target.
	if s.PreviousGeneration != "" {
		oldKey := model.GenerationKey{Tenant: k.Tenant, Generation: s.PreviousGeneration}
		old, err := r.store.RebuildStatus(ctx, oldKey)
		if err != nil {
			return s, err
		}
		if err = r.index.SetProjectionGate(ctx, oldKey, old.Fence, true); err != nil {
			return s, err
		}
	}
	if err = r.index.SetProjectionGate(ctx, k, s.Fence, false); err != nil {
		return s, err
	}
	return r.store.FinishActivation(ctx, k, r.actor, s.Fence)
}

// Abandon fences an unfinished candidate without deleting any database or data.
func (r *Rebuild) Abandon(ctx context.Context, k model.GenerationKey, fence int64) (model.RebuildStatus, error) {
	s, err := r.store.AbandonRebuild(ctx, k, r.actor, fence)
	if err != nil {
		return s, err
	}
	err = r.index.SetProjectionGate(ctx, k, s.Fence, true)
	return s, err
}
