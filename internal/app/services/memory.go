package services

import (
	"context"
	"errors"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/caxqueiroz/facts_engine/model"
)

func memoryTx(tx model.Tx) (model.MemoryTx, error) {
	t, ok := tx.(model.MemoryTx)
	if !ok {
		return nil, model.ErrPrecondition
	}
	return t, nil
}
func (s *Service) RegisterMemoryType(ctx context.Context, c model.Caller, r model.MemoryTypeRequest) (model.CommitResult, error) {
	d := r.Definition
	if !model.ValidID(d.Subtype) || !model.ValidID(d.Version) || !model.ValidID(d.PolicyID) || d.Classification == "" || len(d.Classification) > 256 {
		return model.CommitResult{}, model.ErrInvalid
	}
	if (model.PredicateVersion{Version: d.Version, Migration: d.Migration}).ValidateSchema() != nil {
		return model.CommitResult{}, model.ErrInvalid
	}
	return s.mutate(ctx, c, d.Scope, "register_memory_type", "can_manage_definitions", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := memoryTx(tx)
		if err != nil {
			return err
		}
		if _, err = tx.Policy(ctx, d.Scope, d.PolicyID); err != nil {
			return err
		}
		active, err := t.ActiveMemoryType(ctx, d.Scope, d.Subtype)
		if errors.Is(err, model.ErrNotFound) {
			if d.Migration != nil {
				return model.ErrPrecondition
			}
		} else if err != nil {
			return err
		} else if d.Migration == nil || d.Migration.FromVersion != active {
			return model.ErrPrecondition
		}
		out.ID = d.Version
		return t.StageMemoryType(ctx, d)
	})
}
func (s *Service) memoryDependencyChecks(ctx context.Context, c model.Caller, dependencies []model.MemoryDependency, purpose string) ([]model.AuthorizationDecision, error) {
	checks := []model.AuthorizationDecision{}
	seen := map[string]bool{}
	for _, d := range dependencies {
		if seen[d.Scope] {
			continue
		}
		seen[d.Scope] = true
		for _, permission := range []string{"can_read", "can_read_history"} {
			check, err := s.authorize(ctx, c, d.Scope, permission, model.Command{Purpose: purpose})
			if err != nil {
				return nil, err
			}
			checks = append(checks, check)
		}
	}
	return checks, nil
}
func (s *Service) ProposeMemory(ctx context.Context, c model.Caller, r model.MemoryProposal) (model.CommitResult, error) {
	if !model.ValidID(r.Key.ID) || !model.ValidID(r.Subtype) || !model.ValidID(r.SchemaVersion) || r.ExpectedRevision < 0 || len(r.Body) == 0 || len(r.Body) > 65536 || !utf8.ValidString(r.Body) || r.Valid.Validate() != nil || len(r.Sources) == 0 || len(r.Sources) > 64 || len(r.Dependencies) > 64 {
		return model.CommitResult{}, model.ErrInvalid
	}
	seen := map[string]bool{}
	for _, d := range r.Dependencies {
		if !model.ValidID(d.Scope) || !model.ValidID(d.RecordID) || !model.ValidID(d.VersionID) || !validDigest(d.ContentDigest) || !slices.Contains([]string{"assertion", "uncertain", "disputed"}, d.Label) || seen[d.VersionID] {
			return model.CommitResult{}, model.ErrInvalid
		}
		seen[d.VersionID] = true
	}
	if _, err := s.authorize(ctx, c, r.Key.Scope, "can_propose", r.Command); err != nil {
		return model.CommitResult{}, err
	}
	checks, err := s.memoryDependencyChecks(ctx, c, r.Dependencies, r.Purpose)
	if err != nil {
		return model.CommitResult{}, err
	}
	ids := []string{}
	for _, span := range r.Sources {
		ids = append(ids, span.EvidenceID)
	}
	bytes, err := s.loadEvidence(ctx, c, r.Key.Scope, ids)
	if err != nil {
		return model.CommitResult{}, err
	}
	for _, span := range r.Sources {
		b := bytes[span.EvidenceID]
		if span.Start < 0 || span.End <= span.Start || span.End > len(b) || !utf8.Valid(b[span.Start:span.End]) || string(b[span.Start:span.End]) != span.Quote {
			return model.CommitResult{}, model.ErrInvalid
		}
	}
	return s.mutate(ctx, c, r.Key.Scope, "propose_memory", "can_propose", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		return s.stageMemoryProposal(ctx, tx, c, r, checks, out)
	})
}
func (s *Service) stageMemoryProposal(ctx context.Context, tx model.Tx, c model.Caller, r model.MemoryProposal, checks []model.AuthorizationDecision, out *model.CommitResult) error {
	if err := validateDerivativeSources(ctx, tx, r.Sources); err != nil {
		return err
	}
	t, err := memoryTx(tx)
	if err != nil {
		return err
	}
	active, err := t.ActiveMemoryType(ctx, r.Key.Scope, r.Subtype)
	if err != nil {
		return err
	}
	if active != r.SchemaVersion {
		return model.ErrPrecondition
	}
	definition, err := t.MemoryType(ctx, r.Key.Scope, r.Subtype, r.SchemaVersion)
	if err != nil {
		return err
	}
	for _, d := range r.Dependencies {
		v, err := tx.Version(ctx, d.RecordID, d.VersionID)
		if err != nil {
			return err
		}
		if v.Proposal.Key.Scope != d.Scope || v.Digest != d.ContentDigest {
			return model.ErrPrecondition
		}
	}
	record, err := t.LockMemory(ctx, r.Key)
	if err != nil {
		return err
	}
	if record.Revision != r.ExpectedRevision {
		return &model.RevisionConflict{RecordID: record.ID, ExpectedRevision: r.ExpectedRevision, CurrentRevision: record.Revision}
	}
	for _, check := range checks {
		if time.Since(check.CheckedAt) > 30*time.Second {
			return model.ErrUnavailable
		}
	}
	version := model.MemoryVersion{Authorization: checks, ID: id(), RecordID: record.ID, Proposal: r, Proposer: c, PolicyID: definition.PolicyID, Digest: model.MemoryDigest(r)}
	if err = t.StageMemoryVersion(ctx, version); err != nil {
		return err
	}
	*out = model.CommitResult{ID: version.ID, RecordID: record.ID, VersionID: version.ID, RecordKind: "memory", ContentDigest: version.Digest, StreamRevision: record.Revision + 1}
	return tx.SetRevision(ctx, record.ID, out.StreamRevision)
}
func (s *Service) GetMemoryVersion(ctx context.Context, c model.Caller, q model.MemoryVersionQuery) (model.MemoryVersion, error) {
	empty := model.MemoryVersion{}
	if !model.ValidID(q.RecordID) || !model.ValidID(q.VersionID) {
		return empty, model.ErrInvalid
	}
	authorize := func() error {
		for _, p := range []string{"can_read", "can_read_history"} {
			if _, err := s.authorize(ctx, c, q.Scope, p, model.Command{Purpose: q.Purpose}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := authorize(); err != nil {
		return empty, err
	}
	var v model.MemoryVersion
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := memoryTx(tx)
		if err != nil {
			return err
		}
		v, err = t.MemoryVersion(ctx, q.RecordID, q.VersionID)
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		return empty, model.ErrDenied
	}
	if err != nil {
		return empty, err
	}
	if v.Proposal.Key.Scope != q.Scope {
		return empty, model.ErrDenied
	}
	if model.MemoryDigest(v.Proposal) != v.Digest {
		return empty, model.ErrPrecondition
	}
	if err = s.memoryEvidenceAuthorization(ctx, c, v, q.Purpose); err != nil {
		return empty, err
	}
	if _, err = s.memoryDependencyChecks(ctx, c, v.Proposal.Dependencies, q.Purpose); err != nil {
		return empty, err
	}
	ids := []string{}
	for _, span := range v.Proposal.Sources {
		ids = append(ids, span.EvidenceID)
	}
	if _, err = s.loadEvidence(ctx, c, q.Scope, ids); err != nil {
		return empty, err
	}
	if err = s.memoryEvidenceAuthorization(ctx, c, v, q.Purpose); err != nil {
		return empty, err
	}
	if _, err = s.memoryDependencyChecks(ctx, c, v.Proposal.Dependencies, q.Purpose); err != nil {
		return empty, err
	}
	if err = authorize(); err != nil {
		return empty, err
	}
	return v, nil
}
