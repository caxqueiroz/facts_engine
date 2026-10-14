package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"math"
	"sort"
)

func (s *Service) Recall(ctx context.Context, c model.Caller, q model.RecallRequest) (model.RecallResult, error) {
	out, progress, active, candidates, err := s.searchRecall(ctx, c, q)
	if err != nil {
		return out, err
	}
	scopes := map[string]bool{}
	for _, scope := range q.Scopes {
		scopes[scope] = true
	}
	items := []model.SnapshotItem{}
	scores := []float64{}
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		items = nil
		scores = nil
		revision, at, err := selection(ctx, tx, q.KnowledgeRevision)
		if err != nil {
			return err
		}
		out.LedgerRevision = revision
		seen := map[string]bool{}
		for _, hit := range candidates {
			if hit.Kind != "" && hit.Kind != "fact" {
				continue
			}
			if !model.ValidID(hit.RecordID) || !model.ValidID(hit.VersionID) || math.IsNaN(hit.Score) || math.IsInf(hit.Score, 0) || seen[hit.VersionID] {
				continue
			}
			seen[hit.VersionID] = true
			v, err := tx.Version(ctx, hit.RecordID, hit.VersionID)
			if errors.Is(err, model.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if !scopes[v.Proposal.Key.Scope] {
				continue
			}
			item, err := s.resolve(ctx, tx, model.FactQuery{Key: v.Proposal.Key, ValidAt: q.ValidAt, KnowledgeRevision: revision}, revision, at, q.KnowledgeRevision > 0)
			if err != nil {
				return err
			}
			if item.Resolution.VersionID != hit.VersionID || (item.Resolution.State != "resolved" && item.Resolution.State != "unknown") {
				continue
			}
			items = append(items, item)
			scores = append(scores, hit.Score)
		}
		return nil
	})
	if err != nil {
		return out, err
	}
	for j, item := range items {
		// Recheck permissions after retrieval, before disclosing ledger content.
		if _, err := s.authorize(ctx, c, item.Selector.Scope, "can_read", model.Command{Purpose: q.Purpose}); err != nil {
			return model.RecallResult{}, err
		}
		if q.KnowledgeRevision > 0 {
			if _, err := s.authorize(ctx, c, item.Selector.Scope, "can_read_history", model.Command{Purpose: q.Purpose}); err != nil {
				return model.RecallResult{}, err
			}
		}
		if _, err := s.loadEvidence(ctx, c, item.Selector.Scope, item.EvidenceIDs); err != nil {
			continue
		}
		out.Items = append(out.Items, model.RecallItem{Key: item.Selector, Fact: item.Resolution, Score: scores[j]})
		if len(out.Items) == q.Limit {
			break
		}
	}
	if s.retrieval.Registry != nil {
		now, err := s.retrieval.Registry.ActiveProjection(ctx, c.Tenant)
		if err != nil || now != active {
			return model.RecallResult{}, model.ErrUnavailable
		}
	}
	out.Incomplete = out.IndexedRevision < out.LedgerRevision || progress.State == "failed"
	return out, nil
}
func NewWithRetrieval(l model.Ledger, a model.Authorizer, e model.EvidenceStore, options model.RetrievalOptions) (*Service, error) {
	if options.Index == nil || options.Progress == nil || (options.Registry == nil && !model.ValidID(options.Generation)) || (options.Registry != nil && options.Generation != "") {
		return nil, model.ErrInvalid
	}
	s, err := New(l, a, e)
	if err != nil {
		return nil, err
	}
	s.retrieval = options
	return s, nil
}

func (s *Service) searchRecall(ctx context.Context, c model.Caller, q model.RecallRequest) (model.RecallResult, model.ConsumerStatus, model.ActiveProjection, []model.SearchCandidate, error) {
	var progress model.ConsumerStatus
	var active model.ActiveProjection
	var err error
	out := model.RecallResult{Generation: s.retrieval.Generation, Items: []model.RecallItem{}}
	if s.retrieval.Index == nil || s.retrieval.Progress == nil {
		return out, progress, active, nil, model.ErrUnavailable
	}
	if len(q.Scopes) == 0 || len(q.Scopes) > 8 || len(q.Query) == 0 || len(q.Query) > 4096 || q.Limit < 1 || q.Limit > 20 || q.ValidAt.IsZero() || q.KnowledgeRevision < 0 || q.RequireRevision < 0 {
		return out, progress, active, nil, model.ErrInvalid
	}
	scopes := map[string]bool{}
	for _, scope := range q.Scopes {
		if scopes[scope] {
			return out, progress, active, nil, model.ErrInvalid
		}
		scopes[scope] = true
		if _, err := s.authorize(ctx, c, scope, "can_read", model.Command{Purpose: q.Purpose}); err != nil {
			return out, progress, active, nil, err
		}
		if q.KnowledgeRevision > 0 {
			if _, err := s.authorize(ctx, c, scope, "can_read_history", model.Command{Purpose: q.Purpose}); err != nil {
				return out, progress, active, nil, err
			}
		}
	}
	if s.retrieval.Registry != nil {
		var err error
		active, err = s.retrieval.Registry.ActiveProjection(ctx, c.Tenant)
		if err != nil || active.Generation == "" {
			return out, progress, active, nil, model.ErrUnavailable
		}
		out.Generation = active.Generation
	}
	if q.RequireGeneration != "" && q.RequireGeneration != out.Generation {
		return out, progress, active, nil, model.ErrUnavailable
	}
	progress, err = s.retrieval.Progress.ConsumerStatus(ctx, model.ConsumerKey{Tenant: c.Tenant, Consumer: model.ProjectionConsumer(out.Generation)})
	if err != nil {
		return out, progress, active, nil, model.ErrUnavailable
	}
	out.IndexedRevision = progress.AcknowledgedRevision
	if out.IndexedRevision < q.RequireRevision {
		return out, progress, active, nil, model.ErrUnavailable
	}
	candidates := []model.SearchCandidate{}
	for _, scope := range q.Scopes {
		hits, err := s.retrieval.Index.Candidates(ctx, c.Tenant, out.Generation, scope, q.Query, min(100, q.Limit*5))
		if err != nil {
			return out, progress, active, nil, model.ErrUnavailable
		}
		if len(hits) > 100 {
			return out, progress, active, nil, model.ErrPrecondition
		}
		candidates = append(candidates, hits...)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		return candidates[i].VersionID < candidates[j].VersionID
	})
	return out, progress, active, candidates, nil
}
