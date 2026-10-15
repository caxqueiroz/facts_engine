package services

import (
	"context"
	"errors"
	"math"

	"github.com/caxqueiroz/facts_engine/model"
)

// recallContext resolves both kinds in the same snapshot and preserves the
// shared candidate ranking after eligibility and disclosure filtering.
func (s *Service) recallContext(ctx context.Context, c model.Caller, q model.RecallRequest) (model.RecallResult, []model.MemoryRecallItem, []model.VersionReference, error) {
	out, progress, active, candidates, err := s.searchRecall(ctx, c, q)
	if err != nil {
		return out, nil, nil, err
	}
	type candidate struct {
		fact   *model.SnapshotItem
		memory *memoryRead
		score  float64
	}
	var selected []candidate
	scopes := map[string]bool{}
	for _, scope := range q.Scopes {
		scopes[scope] = true
	}
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		selected = nil
		revision, at, err := selection(ctx, tx, q.KnowledgeRevision)
		if err != nil {
			return err
		}
		out.LedgerRevision = revision
		seen := map[model.VersionReference]bool{}
		for _, hit := range candidates {
			ref := model.VersionReference{RecordID: hit.RecordID, VersionID: hit.VersionID}
			if !model.ValidID(hit.RecordID) || !model.ValidID(hit.VersionID) || math.IsNaN(hit.Score) || math.IsInf(hit.Score, 0) || seen[ref] {
				continue
			}
			seen[ref] = true
			switch hit.Kind {
			case "memory":
				t, err := memoryTx(tx)
				if err != nil {
					return err
				}
				v, err := t.MemoryVersion(ctx, hit.RecordID, hit.VersionID)
				if errors.Is(err, model.ErrNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				if !scopes[v.Proposal.Key.Scope] {
					continue
				}
				view, err := s.resolveMemory(ctx, tx, model.MemoryQuery{Key: v.Proposal.Key, ValidAt: q.ValidAt}, revision, at, q.KnowledgeRevision > 0)
				if err != nil {
					return err
				}
				if view.Result.State == "resolved" && view.Result.VersionID == hit.VersionID {
					selected = append(selected, candidate{memory: &view, score: hit.Score})
				}
			case "", "fact":
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
				if item.Resolution.VersionID == hit.VersionID && (item.Resolution.State == "resolved" || item.Resolution.State == "unknown") {
					selected = append(selected, candidate{fact: &item, score: hit.Score})
				}
			}
		}
		return nil
	})
	if err != nil {
		return model.RecallResult{}, nil, nil, err
	}
	memories := []model.MemoryRecallItem{}
	order := []model.VersionReference{}
	for _, item := range selected {
		if item.memory != nil {
			key := item.memory.Version.Proposal.Key
			resolved, err := s.discloseMemory(ctx, c, model.MemoryQuery{Key: key, ValidAt: q.ValidAt, KnowledgeRevision: q.KnowledgeRevision, Purpose: q.Purpose}, *item.memory)
			if errors.Is(err, model.ErrDenied) || errors.Is(err, model.ErrPrecondition) {
				continue
			}
			if err != nil {
				return model.RecallResult{}, nil, nil, err
			}
			if resolved.State != "resolved" {
				continue
			}
			memories = append(memories, model.MemoryRecallItem{Key: key, Memory: resolved, Score: item.score})
			order = append(order, model.VersionReference{RecordID: resolved.RecordID, VersionID: resolved.VersionID})
		} else {
			fact := item.fact
			if _, err := s.authorize(ctx, c, fact.Selector.Scope, "can_read", model.Command{Purpose: q.Purpose}); err != nil {
				return model.RecallResult{}, nil, nil, err
			}
			if q.KnowledgeRevision > 0 {
				if _, err := s.authorize(ctx, c, fact.Selector.Scope, "can_read_history", model.Command{Purpose: q.Purpose}); err != nil {
					return model.RecallResult{}, nil, nil, err
				}
			}
			if _, err := s.loadEvidence(ctx, c, fact.Selector.Scope, fact.EvidenceIDs); err != nil {
				if interrupted := evidenceDependencyFailure(ctx, err); interrupted != nil {
					return model.RecallResult{}, nil, nil, interrupted
				}
				continue
			}
			out.Items = append(out.Items, model.RecallItem{Key: fact.Selector, Fact: fact.Resolution, Score: item.score})
			order = append(order, model.VersionReference{RecordID: fact.Resolution.RecordID, VersionID: fact.Resolution.VersionID})
		}
		if len(order) == q.Limit {
			break
		}
	}
	if s.retrieval.Registry != nil {
		current, err := s.retrieval.Registry.ActiveProjection(ctx, c.Tenant)
		if err != nil || current != active {
			return model.RecallResult{}, nil, nil, model.ErrUnavailable
		}
	}
	out.Incomplete = out.IndexedRevision < out.LedgerRevision || progress.State == "failed"
	return out, memories, order, nil
}
