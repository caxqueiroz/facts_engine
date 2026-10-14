package services

import (
	"context"
	"errors"
	"math"

	"github.com/caxqueiroz/facts_engine/model"
)

// RecallMemories returns only ledger-revalidated eligible memory versions. Index
// bodies, dependency metadata and eligibility claims are never authoritative.
func (s *Service) RecallMemories(ctx context.Context, c model.Caller, q model.RecallRequest) (model.MemoryRecallResult, error) {
	header, progress, active, candidates, err := s.searchRecall(ctx, c, q)
	out := model.MemoryRecallResult{Generation: header.Generation, IndexedRevision: header.IndexedRevision, Items: []model.MemoryRecallItem{}}
	if err != nil {
		return out, err
	}
	scopes := map[string]bool{}
	for _, scope := range q.Scopes {
		scopes[scope] = true
	}
	views := []memoryRead{}
	scores := []float64{}
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		views = nil
		scores = nil
		t, err := memoryTx(tx)
		if err != nil {
			return err
		}
		revision, at, err := selection(ctx, tx, q.KnowledgeRevision)
		if err != nil {
			return err
		}
		out.LedgerRevision = revision
		seen := map[string]bool{}
		for _, hit := range candidates {
			if hit.Kind != "memory" || !model.ValidID(hit.RecordID) || !model.ValidID(hit.VersionID) || math.IsNaN(hit.Score) || math.IsInf(hit.Score, 0) || seen[hit.VersionID] {
				continue
			}
			seen[hit.VersionID] = true
			version, err := t.MemoryVersion(ctx, hit.RecordID, hit.VersionID)
			if errors.Is(err, model.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if !scopes[version.Proposal.Key.Scope] {
				continue
			}
			view, err := s.resolveMemory(ctx, tx, model.MemoryQuery{Key: version.Proposal.Key, ValidAt: q.ValidAt}, revision, at, q.KnowledgeRevision > 0)
			if err != nil {
				return err
			}
			if view.Result.State != "resolved" || view.Result.VersionID != hit.VersionID {
				continue
			}
			views = append(views, view)
			scores = append(scores, hit.Score)
		}
		return nil
	})
	if err != nil {
		return model.MemoryRecallResult{}, err
	}
	for i, view := range views {
		query := model.MemoryQuery{Key: view.Version.Proposal.Key, ValidAt: q.ValidAt, KnowledgeRevision: q.KnowledgeRevision, Purpose: q.Purpose}
		resolved, err := s.discloseMemory(ctx, c, query, view)
		if errors.Is(err, model.ErrDenied) || errors.Is(err, model.ErrPrecondition) {
			continue
		}
		if err != nil {
			return model.MemoryRecallResult{}, err
		}
		if resolved.State != "resolved" {
			continue
		}
		out.Items = append(out.Items, model.MemoryRecallItem{Key: query.Key, Memory: resolved, Score: scores[i]})
		if len(out.Items) == q.Limit {
			break
		}
	}
	if s.retrieval.Registry != nil {
		current, err := s.retrieval.Registry.ActiveProjection(ctx, c.Tenant)
		if err != nil || current != active {
			return model.MemoryRecallResult{}, model.ErrUnavailable
		}
	}
	out.Incomplete = out.IndexedRevision < out.LedgerRevision || progress.State == "failed"
	return out, nil
}
