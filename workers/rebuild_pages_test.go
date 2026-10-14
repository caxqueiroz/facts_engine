package workers

import (
	"context"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

type pagedRebuildStore struct {
	model.RebuildStore
	status   model.RebuildStatus
	verified bool
}

func (s *pagedRebuildStore) FreezeRebuild(context.Context, model.GenerationKey, model.RebuildActor) (model.RebuildStatus, error) {
	return s.status, nil
}
func (s *pagedRebuildStore) ProjectionDocumentPage(_ context.Context, k model.GenerationKey, fence, through int64, after model.ProjectionCursor, limit int) (model.ProjectionPage, error) {
	if k != s.status.Key || fence != s.status.Fence || through != s.status.ThroughRevision || after != (model.ProjectionCursor{}) || limit != 100 {
		return model.ProjectionPage{}, model.ErrInvalid
	}
	return model.ProjectionPage{Documents: []model.ProjectionDocument{}}, nil
}
func (s *pagedRebuildStore) VerifyRebuild(_ context.Context, _ model.GenerationKey, _ model.RebuildActor, _ int64, check model.ProjectionCheck) (model.RebuildStatus, error) {
	s.verified = true
	s.status.State = "verified"
	s.status.VerificationVersion = check.Version
	s.status.Count = check.Count
	s.status.Digest = check.Digest
	return s.status, nil
}

type pagedRebuildIndex struct {
	model.RebuildIndex
	version string
	sealed  bool
}

func (i *pagedRebuildIndex) SetProjectionGate(_ context.Context, _ model.GenerationKey, _ int64, sealed bool) error {
	i.sealed = sealed
	return nil
}
func (i *pagedRebuildIndex) VerifyProjectionPages(ctx context.Context, _ model.GenerationKey, _ int64, load model.ProjectionPageLoader) (model.ProjectionCheck, error) {
	if !i.sealed {
		return model.ProjectionCheck{}, model.ErrPrecondition
	}
	if _, err := load(ctx, model.ProjectionCursor{}); err != nil {
		return model.ProjectionCheck{}, err
	}
	check := model.NewProjectionInventory().Check()
	check.Version = i.version
	return check, nil
}
func TestRebuildRequiresVersionedPagedProof(t *testing.T) {
	for _, version := range []string{model.ProjectionInventoryV2, "", "unknown"} {
		t.Run(version, func(t *testing.T) {
			key := model.GenerationKey{Tenant: "tenant", Generation: "generation"}
			store := &pagedRebuildStore{status: model.RebuildStatus{Key: key, State: "paused", Fence: 7, ThroughRevision: 20}}
			index := &pagedRebuildIndex{version: version}
			worker, err := NewRebuild(store, index, model.RebuildActor{Principal: "operator", Purpose: "rebuild"})
			if err != nil {
				t.Fatal(err)
			}
			status, err := worker.Verify(context.Background(), key)
			if version == model.ProjectionInventoryV2 {
				if err != nil || !store.verified || status.VerificationVersion != version {
					t.Fatal(status, err)
				}
			} else if err != model.ErrPrecondition || store.verified {
				t.Fatal("unversioned paged receipt committed", err)
			}
		})
	}
}
