package services

import (
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) ApproveMemory(ctx context.Context, c model.Caller, r model.Approval) (model.CommitResult, error) {
	return s.acceptMemory(ctx, c, r, nil, false)
}
func (s *Service) AcceptMemory(ctx context.Context, c model.Caller, r model.Approval) (model.CommitResult, error) {
	return s.acceptMemory(ctx, c, r, nil, true)
}
func (s *Service) SupersedeMemory(ctx context.Context, c model.Caller, r model.Supersession) (model.CommitResult, error) {
	if !model.ValidID(r.Reason) || r.Valid.From == nil || r.Valid.Validate() != nil || len(r.ReplacedVersionIDs) == 0 || len(r.ReplacedVersionIDs) > 64 {
		return model.CommitResult{}, model.ErrInvalid
	}
	return s.acceptMemory(ctx, c, r.Approval, &r, r.Automated)
}
func (s *Service) memoryCandidate(ctx context.Context, c model.Caller, r model.Approval, permission string) (model.MemoryVersion, error) {
	var v model.MemoryVersion
	if !model.ValidID(r.RecordID) || !model.ValidID(r.VersionID) || r.ExpectedRevision < 0 {
		return v, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, r.Scope, permission, r.Command); err != nil {
		return v, err
	}
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := memoryTx(tx)
		if err != nil {
			return err
		}
		v, err = t.MemoryVersion(ctx, r.RecordID, r.VersionID)
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		return v, model.ErrDenied
	}
	if err != nil {
		return v, err
	}
	if v.Proposal.Key.Scope != r.Scope {
		return model.MemoryVersion{}, model.ErrDenied
	}
	if model.MemoryDigest(v.Proposal) != v.Digest {
		return model.MemoryVersion{}, model.ErrPrecondition
	}
	return v, nil
}
func lockMemoryInputs(ctx context.Context, t model.MemoryLifecycleTx, v model.MemoryVersion) (model.Record, error) {
	keys := map[string]model.FactKey{}
	ids := []string{v.RecordID}
	for _, dependency := range v.Proposal.Dependencies {
		if _, ok := keys[dependency.RecordID]; ok {
			continue
		}
		fact, err := t.Version(ctx, dependency.RecordID, dependency.VersionID)
		if err != nil {
			return model.Record{}, err
		}
		keys[dependency.RecordID] = fact.Proposal.Key
		ids = append(ids, dependency.RecordID)
	}
	sort.Strings(ids)
	var memory model.Record
	for _, id := range ids {
		if id == v.RecordID {
			record, err := t.LockMemory(ctx, v.Proposal.Key)
			if err != nil {
				return memory, err
			}
			memory = record
		} else {
			record, err := t.LockRecord(ctx, keys[id])
			if err != nil {
				return memory, err
			}
			if record.ID != id {
				return memory, model.ErrPrecondition
			}
		}
	}
	return memory, nil
}
func (s *Service) acceptMemory(ctx context.Context, c model.Caller, r model.Approval, replacement *model.Supersession, automatic bool) (model.CommitResult, error) {
	op, permission := "approve_memory", "can_approve"
	if automatic {
		op, permission = "accept_memory", "can_accept"
	}
	var input any = r
	if replacement != nil {
		op = "supersede_memory"
		input = *replacement
	}
	v, err := s.memoryCandidate(ctx, c, r, permission)
	if err != nil {
		return model.CommitResult{}, err
	}
	if replacement != nil && !sameInterval(replacement.Valid, v.Proposal.Valid) {
		return model.CommitResult{}, model.ErrInvalid
	}
	checks, err := s.memoryDependencyChecks(ctx, c, v.Proposal.Dependencies, r.Purpose)
	if err != nil {
		return model.CommitResult{}, err
	}
	ids := []string{}
	for _, span := range v.Proposal.Sources {
		ids = append(ids, span.EvidenceID)
	}
	if _, err = s.loadEvidence(ctx, c, r.Scope, ids); err != nil {
		return model.CommitResult{}, err
	}
	if err = s.memoryDependencyEvidence(ctx, c, v); err != nil {
		return model.CommitResult{}, err
	}
	verified, err := s.verifiedPrerequisites(ctx, c, r.Scope, v.ID, v.Digest, v.PolicyID, true)
	if err != nil {
		return model.CommitResult{}, err
	}
	return s.mutate(ctx, c, r.Scope, op, permission, r.Command, input, func(tx model.Tx, out *model.CommitResult) error {
		t, err := memoryLifecycleTx(tx)
		if err != nil {
			return err
		}
		active, err := t.ActiveMemoryType(ctx, r.Scope, v.Proposal.Subtype)
		if err != nil {
			return err
		}
		if active != v.Proposal.SchemaVersion {
			return model.ErrPrecondition
		}
		record, err := lockMemoryInputs(ctx, t, v)
		if err != nil {
			return err
		}
		if record.Revision != r.ExpectedRevision {
			return &model.RevisionConflict{RecordID: record.ID, ExpectedRevision: r.ExpectedRevision, CurrentRevision: record.Revision}
		}
		rejected, err := t.MemoryRejected(ctx, record.ID, v.ID, 0)
		if err != nil {
			return err
		}
		accepted, err := t.MemoryHasAcceptance(ctx, record.ID, v.ID)
		if err != nil {
			return err
		}
		if rejected || accepted || v.Proposal.Valid.From == nil {
			return model.ErrPrecondition
		}
		policy, err := tx.Policy(ctx, r.Scope, v.PolicyID)
		if err != nil {
			return err
		}
		if automatic {
			if policy.Mode != "automated" || !c.Service || !slices.Contains(policy.AcceptingServices, c.Principal) {
				return model.ErrPrecondition
			}
		} else if policy.Mode != "manual" || c.Principal == v.Proposer.Principal || c.Human() == v.Proposer.Human() {
			return model.ErrPrecondition
		}
		attestations, err := t.MemoryAttestations(ctx, v.ID, 0)
		if err != nil {
			return err
		}
		attestationIDs, err := versionPrerequisites(policy, v.ID, v.Digest, attestations, time.Now())
		if err != nil {
			return err
		}
		if err := requireVerifiedPrerequisites(attestationIDs, verified); err != nil {
			return err
		}
		for _, span := range v.Proposal.Sources {
			evidence, err := tx.Evidence(ctx, span.EvidenceID)
			if err != nil {
				return err
			}
			if evidence.SourceClass == "assistant" || evidence.SourceClass == "system" || !slices.Contains(policy.SourceClasses, evidence.SourceClass) {
				return model.ErrPrecondition
			}
		}
		eligible, err := s.memoryDependencies(ctx, t, v, 0, time.Now(), false)
		if err != nil {
			return err
		}
		if !eligible {
			return model.ErrPrecondition
		}
		for _, check := range checks {
			if time.Since(check.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		entries, err := t.MemoryTimeline(ctx, record.ID, 0)
		if err != nil {
			return err
		}
		overlaps := map[string]bool{}
		for _, entry := range entries {
			if entry.Valid.Overlaps(v.Proposal.Valid) {
				overlaps[entry.VersionID] = true
				if replacement == nil && entry.State != "retracted" {
					return model.ErrPrecondition
				}
			}
		}
		if replacement != nil {
			supplied := map[string]bool{}
			for _, id := range replacement.ReplacedVersionIDs {
				if supplied[id] || !overlaps[id] {
					return model.ErrPrecondition
				}
				supplied[id] = true
			}
			if len(supplied) != len(overlaps) {
				return model.ErrPrecondition
			}
		}
		next := []model.Acceptance{}
		for _, entry := range entries {
			for _, part := range model.Subtract(entry.Valid, v.Proposal.Valid) {
				piece := entry
				piece.Valid = part
				next = append(next, piece)
			}
		}
		next = append(next, model.Acceptance{VersionID: v.ID, Valid: v.Proposal.Valid, PolicyID: policy.ID, State: "accepted", DependencyAuthorization: checks, AttestationIDs: attestationIDs})
		model.SortTimeline(next)
		if err = t.StageMemoryTimeline(ctx, record.ID, next); err != nil {
			return err
		}
		*out = model.CommitResult{ID: v.ID, RecordID: record.ID, VersionID: v.ID, RecordKind: "memory", ContentDigest: v.Digest, StreamRevision: record.Revision + 1}
		return tx.SetRevision(ctx, record.ID, out.StreamRevision)
	})
}
func (s *Service) RejectMemory(ctx context.Context, c model.Caller, r model.LifecycleChange) (model.CommitResult, error) {
	return s.changeMemory(ctx, c, r, "reject_memory", "can_approve")
}
func (s *Service) DisputeMemory(ctx context.Context, c model.Caller, r model.LifecycleChange) (model.CommitResult, error) {
	return s.changeMemory(ctx, c, r, "dispute_memory", "can_dispute")
}
func (s *Service) ResolveMemoryDispute(ctx context.Context, c model.Caller, r model.LifecycleChange) (model.CommitResult, error) {
	return s.changeMemory(ctx, c, r, "resolve_memory_dispute", "can_resolve")
}
func (s *Service) RetractMemory(ctx context.Context, c model.Caller, r model.LifecycleChange) (model.CommitResult, error) {
	return s.changeMemory(ctx, c, r, "retract_memory", "can_resolve")
}
func (s *Service) changeMemory(ctx context.Context, c model.Caller, r model.LifecycleChange, op, permission string) (model.CommitResult, error) {
	if !model.ValidID(r.Reason) || (op != "reject_memory" && (r.Valid.From == nil || r.Valid.Validate() != nil)) {
		return model.CommitResult{}, model.ErrInvalid
	}
	v, err := s.memoryCandidate(ctx, c, r.Approval, permission)
	if err != nil {
		return model.CommitResult{}, err
	}
	return s.mutate(ctx, c, r.Scope, op, permission, r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := memoryLifecycleTx(tx)
		if err != nil {
			return err
		}
		record, err := t.LockMemory(ctx, v.Proposal.Key)
		if err != nil {
			return err
		}
		if record.Revision != r.ExpectedRevision {
			return &model.RevisionConflict{RecordID: record.ID, ExpectedRevision: r.ExpectedRevision, CurrentRevision: record.Revision}
		}
		if op == "reject_memory" {
			rejected, err := t.MemoryRejected(ctx, record.ID, v.ID, 0)
			if err != nil {
				return err
			}
			accepted, err := t.MemoryHasAcceptance(ctx, record.ID, v.ID)
			if err != nil {
				return err
			}
			if rejected || accepted {
				return model.ErrPrecondition
			}
			if err = t.StageMemoryRejection(ctx, record.ID, v.ID); err != nil {
				return err
			}
		} else {
			entries, err := t.MemoryTimeline(ctx, record.ID, 0)
			if err != nil {
				return err
			}
			next := []model.Acceptance{}
			covered := r.Valid.From
			complete := false
			for _, entry := range entries {
				if entry.VersionID != v.ID || !entry.Valid.Overlaps(r.Valid) {
					next = append(next, entry)
					continue
				}
				allowed := op == "dispute_memory" && entry.State == "accepted" || op == "resolve_memory_dispute" && entry.State == "disputed" || op == "retract_memory" && (entry.State == "accepted" || entry.State == "disputed")
				if !allowed {
					return model.ErrPrecondition
				}
				hit := model.Intersect(entry.Valid, r.Valid)
				if covered == nil || !covered.Equal(*hit.From) {
					return model.ErrPrecondition
				}
				covered = hit.To
				if hit.To == nil || r.Valid.To != nil && hit.To.Equal(*r.Valid.To) {
					complete = true
				}
				for _, part := range model.Subtract(entry.Valid, r.Valid) {
					piece := entry
					piece.Valid = part
					next = append(next, piece)
				}
				entry.Valid = hit
				switch op {
				case "dispute_memory":
					entry.State = "disputed"
				case "resolve_memory_dispute":
					entry.State = "accepted"
				case "retract_memory":
					entry.State = "retracted"
				}
				next = append(next, entry)
			}
			if !complete {
				return model.ErrPrecondition
			}
			model.SortTimeline(next)
			if err = t.StageMemoryTimeline(ctx, record.ID, next); err != nil {
				return err
			}
		}
		*out = model.CommitResult{ID: v.ID, RecordID: record.ID, VersionID: v.ID, RecordKind: "memory", StreamRevision: record.Revision + 1}
		return tx.SetRevision(ctx, record.ID, out.StreamRevision)
	})
}
