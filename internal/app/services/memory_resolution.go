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
func (s *Service) GetMemory(ctx context.Context, c model.Caller, q model.MemoryQuery) (model.MemoryResult, error) {
	empty := model.MemoryResult{}
	if !model.ValidID(q.Key.ID) || q.ValidAt.IsZero() || q.KnowledgeRevision < 0 || (q.KnownAt != nil && (q.KnownAt.IsZero() || q.KnowledgeRevision != 0)) {
		return empty, model.ErrInvalid
	}
	historical := q.KnowledgeRevision > 0 || q.KnownAt != nil
	authorize := func() error {
		if _, err := s.authorize(ctx, c, q.Key.Scope, "can_read", model.Command{Purpose: q.Purpose}); err != nil {
			return err
		}
		if historical {
			if _, err := s.authorize(ctx, c, q.Key.Scope, "can_read_history", model.Command{Purpose: q.Purpose}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := authorize(); err != nil {
		return empty, err
	}
	out := model.MemoryResult{}
	var selected *model.MemoryVersion
	var attestationEvidence []string
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		out = model.MemoryResult{State: "missing"}
		selected = nil
		attestationEvidence = nil
		t, err := memoryLifecycleTx(tx)
		if err != nil {
			return err
		}
		revision, at, err := readSelection(ctx, tx, q.KnowledgeRevision, q.KnownAt)
		if err != nil {
			return err
		}
		out.LedgerRevision = revision
		if revision == 0 {
			return nil
		}
		record, err := tx.Record(ctx, model.MemoryRecordID(q.Key))
		if errors.Is(err, model.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if record.Kind != "memory" {
			return model.ErrPrecondition
		}
		stream, err := t.MemoryStreamRevisionAt(ctx, record.ID, revision)
		if err != nil {
			return err
		}
		if stream == 0 {
			return nil
		}
		out.RecordID = record.ID
		out.StreamRevision = stream
		out.State = "unresolved"
		entries, err := t.MemoryTimeline(ctx, record.ID, revision)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !entry.Valid.Contains(q.ValidAt) {
				continue
			}
			out.VersionID = entry.VersionID
			v, err := t.MemoryVersion(ctx, record.ID, entry.VersionID)
			if err != nil {
				return err
			}
			selected = &v
			if model.MemoryDigest(v.Proposal) != v.Digest {
				return model.ErrPrecondition
			}
			if entry.State == "disputed" || entry.State == "retracted" {
				out.State = entry.State
				return nil
			}
			policy, err := tx.Policy(ctx, q.Key.Scope, entry.PolicyID)
			if err != nil {
				return err
			}
			attestations, err := t.MemoryAttestations(ctx, v.ID, revision)
			if err != nil {
				return err
			}
			bound := []model.Attestation{}
			for _, a := range attestations {
				if slices.Contains(entry.AttestationIDs, a.ID) {
					bound = append(bound, a)
					attestationEvidence = append(attestationEvidence, a.EvidenceID)
				}
			}
			if _, err = versionPrerequisites(policy, v.ID, v.Digest, bound, at); err != nil {
				out.State = "ineligible"
				return nil
			}
			eligible, err := s.memoryDependencies(ctx, t, v, revision, at, historical)
			if err != nil {
				return err
			}
			if !eligible {
				out.State = "stale"
				return nil
			}
			out.State = "resolved"
			out.Body = v.Proposal.Body
			out.Dependencies = v.Proposal.Dependencies
			return nil
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	if selected != nil {
		if _, err = s.memoryDependencyChecks(ctx, c, selected.Proposal.Dependencies, q.Purpose); err != nil {
			return empty, err
		}
		if out.State == "resolved" {
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
	if err = authorize(); err != nil {
		return empty, err
	}
	return out, nil
}
