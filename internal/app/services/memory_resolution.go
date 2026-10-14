package services

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func memoryLifecycleTx(tx model.Tx) (model.MemoryLifecycleTx, error) {
	t, ok := tx.(model.MemoryLifecycleTx)
	if !ok {
		return nil, model.ErrPrecondition
	}
	return t, nil
}

// memoryDependencies requires coverage of the entire interval described by the
// memory. A point-in-time match alone cannot establish a summary's eligibility.
func (s *Service) memoryDependencies(ctx context.Context, t model.MemoryLifecycleTx, v model.MemoryVersion, revision int64, at time.Time, historical bool) (bool, error) {
	if v.Proposal.Valid.From == nil {
		return false, nil
	}
	for _, dependency := range v.Proposal.Dependencies {
		fact, err := t.Version(ctx, dependency.RecordID, dependency.VersionID)
		if err != nil {
			return false, err
		}
		if fact.Digest != dependency.ContentDigest || fact.Proposal.Key.Scope != dependency.Scope {
			return false, model.ErrPrecondition
		}
		entries, err := t.Acceptances(ctx, dependency.RecordID, revision)
		if err != nil {
			return false, err
		}
		uncovered := []model.Interval{v.Proposal.Valid}
		for _, entry := range entries {
			if entry.VersionID != fact.ID || !entry.Valid.Overlaps(v.Proposal.Valid) {
				continue
			}
			allowed := dependency.Label == "assertion" && entry.State == "accepted" || dependency.Label == "disputed" && entry.State == "disputed" || dependency.Label == "uncertain" && entry.State == "accepted" && fact.Proposal.Value.Kind == model.Unknown
			if !allowed {
				continue
			}
			policy, err := t.Policy(ctx, dependency.Scope, entry.PolicyID)
			if err != nil {
				return false, err
			}
			var attestations []model.Attestation
			if historical {
				attestations, err = t.HistoricalAttestations(ctx, fact.ID, revision)
			} else {
				attestations, err = t.Attestations(ctx, fact.ID)
			}
			if err != nil {
				return false, err
			}
			bound := []model.Attestation{}
			for _, att := range attestations {
				if slices.Contains(entry.AttestationIDs, att.ID) {
					bound = append(bound, att)
				}
			}
			if _, err = prerequisites(policy, fact, bound, at); err != nil {
				continue
			}
			remaining := []model.Interval{}
			for _, part := range uncovered {
				remaining = append(remaining, model.Subtract(part, entry.Valid)...)
			}
			uncovered = remaining
		}
		if len(uncovered) > 0 {
			return false, nil
		}
	}
	return true, nil
}
func (s *Service) memoryDependencyEvidence(ctx context.Context, c model.Caller, v model.MemoryVersion) error {
	sources := map[string][]string{}
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		sources = map[string][]string{}
		for _, dependency := range v.Proposal.Dependencies {
			fact, err := tx.Version(ctx, dependency.RecordID, dependency.VersionID)
			if err != nil {
				return err
			}
			if fact.Proposal.Key.Scope != dependency.Scope || fact.Digest != dependency.ContentDigest {
				return model.ErrPrecondition
			}
			for _, span := range fact.Proposal.Sources {
				sources[dependency.Scope] = append(sources[dependency.Scope], span.EvidenceID)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for scope, ids := range sources {
		if _, err = s.loadEvidence(ctx, c, scope, ids); err != nil {
			return err
		}
	}
	return nil
}

type memoryRead struct {
	Result              model.MemoryResult
	Version             *model.MemoryVersion
	AttestationEvidence []string
}

func (s *Service) memoryReadAuthorization(ctx context.Context, c model.Caller, q model.MemoryQuery) error {
	if _, err := s.authorize(ctx, c, q.Key.Scope, "can_read", model.Command{Purpose: q.Purpose}); err != nil {
		return err
	}
	if q.KnowledgeRevision > 0 || q.KnownAt != nil {
		if _, err := s.authorize(ctx, c, q.Key.Scope, "can_read_history", model.Command{Purpose: q.Purpose}); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) GetMemory(ctx context.Context, c model.Caller, q model.MemoryQuery) (model.MemoryResult, error) {
	if !model.ValidID(q.Key.ID) || q.ValidAt.IsZero() || q.KnowledgeRevision < 0 || (q.KnownAt != nil && (q.KnownAt.IsZero() || q.KnowledgeRevision != 0)) {
		return model.MemoryResult{}, model.ErrInvalid
	}
	if err := s.memoryReadAuthorization(ctx, c, q); err != nil {
		return model.MemoryResult{}, err
	}
	var view memoryRead
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		revision, at, err := readSelection(ctx, tx, q.KnowledgeRevision, q.KnownAt)
		if err != nil {
			return err
		}
		view, err = s.resolveMemory(ctx, tx, q, revision, at, q.KnowledgeRevision > 0 || q.KnownAt != nil)
		return err
	})
	if err != nil {
		return model.MemoryResult{}, err
	}
	return s.discloseMemory(ctx, c, q, view)
}
func (s *Service) resolveMemory(ctx context.Context, tx model.Tx, q model.MemoryQuery, revision int64, at time.Time, historical bool) (memoryRead, error) {
	view := memoryRead{Result: model.MemoryResult{State: "missing", LedgerRevision: revision}}
	t, err := memoryLifecycleTx(tx)
	if err != nil {
		return view, err
	}

	if revision == 0 {
		return view, nil
	}
	record, err := tx.Record(ctx, model.MemoryRecordID(q.Key))
	if errors.Is(err, model.ErrNotFound) {
		return view, nil
	}
	if err != nil {
		return view, err
	}
	if record.Kind != "memory" {
		return view, model.ErrPrecondition
	}
	stream, err := t.MemoryStreamRevisionAt(ctx, record.ID, revision)
	if err != nil {
		return view, err
	}
	if stream == 0 {
		return view, nil
	}
	view.Result.RecordID = record.ID
	view.Result.StreamRevision = stream
	view.Result.State = "unresolved"
	entries, err := t.MemoryTimeline(ctx, record.ID, revision)
	if err != nil {
		return view, err
	}
	for _, entry := range entries {
		if !entry.Valid.Contains(q.ValidAt) {
			continue
		}
		view.Result.VersionID = entry.VersionID
		v, err := t.MemoryVersion(ctx, record.ID, entry.VersionID)
		if err != nil {
			return view, err
		}
		view.Version = &v
		if model.MemoryDigest(v.Proposal) != v.Digest {
			return view, model.ErrPrecondition
		}
		if entry.State == "disputed" || entry.State == "retracted" {
			view.Result.State = entry.State
			return view, nil
		}
		policy, err := tx.Policy(ctx, q.Key.Scope, entry.PolicyID)
		if err != nil {
			return view, err
		}
		attestations, err := t.MemoryAttestations(ctx, v.ID, revision)
		if err != nil {
			return view, err
		}
		bound := []model.Attestation{}
		for _, a := range attestations {
			if slices.Contains(entry.AttestationIDs, a.ID) {
				bound = append(bound, a)
				view.AttestationEvidence = append(view.AttestationEvidence, model.AttestationEvidence(a)...)
			}
		}
		if _, err = versionPrerequisites(policy, v.ID, v.Digest, bound, at); err != nil {
			view.Result.State = "ineligible"
			return view, nil
		}
		eligible, err := s.memoryDependencies(ctx, t, v, revision, at, historical)
		if err != nil {
			return view, err
		}
		if !eligible {
			view.Result.State = "stale"
			return view, nil
		}
		view.Result.State = "resolved"
		view.Result.Body = v.Proposal.Body
		view.Result.Dependencies = v.Proposal.Dependencies
		return view, nil
	}
	return view, nil
}
func (s *Service) discloseMemory(ctx context.Context, c model.Caller, q model.MemoryQuery, view memoryRead) (model.MemoryResult, error) {
	empty := model.MemoryResult{}
	out, selected, attestationEvidence := view.Result, view.Version, view.AttestationEvidence
	var err error
	if selected != nil {
		if _, err = s.memoryDependencyChecks(ctx, c, selected.Proposal.Dependencies, q.Purpose); err != nil {
			return empty, err
		}
		if out.State == "resolved" {
			if err = s.memoryEvidenceAuthorization(ctx, c, *selected, q.Purpose); err != nil {
				return empty, err
			}
			ids := append([]string{}, attestationEvidence...)
			for _, span := range selected.Proposal.Sources {
				ids = append(ids, span.EvidenceID)
			}
			_, err = s.loadEvidence(ctx, c, q.Key.Scope, ids)
			if err == nil {
				err = s.memoryDependencyEvidence(ctx, c, *selected)
			}
			if err != nil {
				if !errors.Is(err, model.ErrPrecondition) {
					return empty, err
				}
				out.State = "ineligible"
				out.Body = ""
				out.Dependencies = nil
			}
		}
	}
	if selected != nil {
		if _, err = s.memoryDependencyChecks(ctx, c, selected.Proposal.Dependencies, q.Purpose); err != nil {
			return empty, err
		}
	}
	if selected != nil && out.State == "resolved" {
		if err = s.memoryEvidenceAuthorization(ctx, c, *selected, q.Purpose); err != nil {
			return empty, err
		}
	}
	if err = s.memoryReadAuthorization(ctx, c, q); err != nil {
		return empty, err
	}
	return out, nil
}

func (s *Service) memoryEvidenceAuthorization(ctx context.Context, c model.Caller, v model.MemoryVersion, purpose string) error {
	scopes := map[string]bool{v.Proposal.Key.Scope: true}
	for _, dependency := range v.Proposal.Dependencies {
		scopes[dependency.Scope] = true
	}
	for scope := range scopes {
		if _, err := s.authorize(ctx, c, scope, "can_read_evidence", model.Command{Purpose: purpose}); err != nil {
			return err
		}
	}
	return nil
}
