package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"slices"
	"time"
)

func temporal(tx model.Tx) (model.TemporalTx, error) {
	t, ok := tx.(model.TemporalTx)
	if !ok {
		return nil, model.ErrPrecondition
	}
	return t, nil
}
func sameInterval(a, b model.Interval) bool {
	return a.From != nil && b.From != nil && a.From.Equal(*b.From) && ((a.To == nil && b.To == nil) || (a.To != nil && b.To != nil && a.To.Equal(*b.To)))
}
func (s *Service) candidate(ctx context.Context, c model.Caller, r model.Approval, permission string) (model.Version, error) {
	if _, err := s.authorize(ctx, c, r.Scope, permission, r.Command); err != nil {
		return model.Version{}, err
	}
	var v model.Version
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error { var err error; v, err = tx.Version(ctx, r.RecordID, r.VersionID); return err })
	if err != nil {
		return v, err
	}
	if v.Proposal.Key.Scope != r.Scope {
		return v, model.ErrDenied
	}
	ids := []string{}
	for _, span := range v.Proposal.Sources {
		ids = append(ids, span.EvidenceID)
	}
	_, err = s.loadEvidence(ctx, c, r.Scope, ids)
	return v, err
}
func (s *Service) acceptanceChecks(ctx context.Context, tx model.Tx, c model.Caller, rec model.Record, v model.Version, automatic bool) (model.AcceptancePolicy, []string, error) {
	p := model.AcceptancePolicy{}
	t, err := temporal(tx)
	if err != nil {
		return p, nil, err
	}
	rejected, err := t.Rejected(ctx, rec.ID, v.ID, 0)
	if err != nil {
		return p, nil, err
	}
	accepted, err := t.HasAcceptance(ctx, rec.ID, v.ID)
	if err != nil {
		return p, nil, err
	}
	if rejected || accepted {
		return p, nil, model.ErrPrecondition
	}
	active, err := tx.ActivePredicate(ctx, v.Proposal.Key.Scope, v.Proposal.Key.Predicate)
	if err != nil {
		return p, nil, err
	}
	if active != v.Proposal.SchemaVersion {
		return p, nil, model.ErrPrecondition
	}
	definition, err := tx.Predicate(ctx, v.Proposal.Key.Scope, v.Proposal.Key.Predicate, v.Proposal.SchemaVersion)
	if err != nil {
		return p, nil, err
	}
	if definition.ValidateFact(v.Proposal.Value, v.Proposal.Valid) != nil {
		return p, nil, model.ErrPrecondition
	}
	p, err = tx.Policy(ctx, v.Proposal.Key.Scope, v.PolicyID)
	if err != nil {
		return p, nil, err
	}
	if automatic {
		if p.Mode != "automated" || !c.Service || !slices.Contains(p.AcceptingServices, c.Principal) {
			return p, nil, model.ErrPrecondition
		}
	} else {
		if p.Mode != "manual" || c.Principal == v.Proposer.Principal || c.Human() == v.Proposer.Human() {
			return p, nil, model.ErrPrecondition
		}
	}
	if v.Proposal.Valid.From == nil {
		return p, nil, model.ErrPrecondition
	}
	for _, span := range v.Proposal.Sources {
		e, err := tx.Evidence(ctx, span.EvidenceID)
		if err != nil {
			return p, nil, err
		}
		if (e.SourceClass == "assistant" || e.SourceClass == "system") || !slices.Contains(p.SourceClasses, e.SourceClass) {
			return p, nil, model.ErrPrecondition
		}
	}
	as, err := tx.Attestations(ctx, v.ID)
	if err != nil {
		return p, nil, err
	}
	ids, err := prerequisites(p, v, as, time.Now())
	return p, ids, err
}
func (s *Service) SupersedeFact(ctx context.Context, c model.Caller, r model.Supersession) (model.CommitResult, error) {
	permission := "can_approve"
	if r.Automated {
		permission = "can_accept"
	}
	if !model.ValidID(r.Reason) || r.Valid.From == nil || r.Valid.Validate() != nil || len(r.ReplacedVersionIDs) == 0 {
		return model.CommitResult{}, model.ErrInvalid
	}
	v, err := s.candidate(ctx, c, r.Approval, permission)
	if err != nil {
		return model.CommitResult{}, err
	}
	if !sameInterval(r.Valid, v.Proposal.Valid) {
		return model.CommitResult{}, model.ErrInvalid
	}
	verified, err := s.verifiedPrerequisites(ctx, c, r.Scope, v.ID, v.Digest, v.PolicyID, false)
	if err != nil {
		return model.CommitResult{}, err
	}
	var commitPolicy *model.AcceptancePolicy
	var commitIDs []string
	return s.mutate(ctx, c, r.Scope, "supersede_fact", permission, r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := temporal(tx)
		if err != nil {
			return err
		}
		rec, err := tx.LockRecord(ctx, v.Proposal.Key)
		if err != nil {
			return err
		}
		if rec.Revision != r.ExpectedRevision {
			return &model.RevisionConflict{RecordID: rec.ID, ExpectedRevision: r.ExpectedRevision, CurrentRevision: rec.Revision}
		}
		p, attIDs, err := s.acceptanceChecks(ctx, tx, c, rec, v, r.Automated)
		if err != nil {
			return err
		}
		if err := requireVerifiedPrerequisites(attIDs, verified); err != nil {
			return err
		}
		commitPolicy = &p
		commitIDs = attIDs
		entries, err := tx.Acceptances(ctx, rec.ID, 0)
		if err != nil {
			return err
		}
		overlaps := map[string]bool{}
		for _, a := range entries {
			if a.Valid.Overlaps(r.Valid) {
				overlaps[a.VersionID] = true
			}
		}
		supplied := map[string]bool{}
		for _, id := range r.ReplacedVersionIDs {
			if supplied[id] || !overlaps[id] {
				return model.ErrPrecondition
			}
			supplied[id] = true
		}
		if len(supplied) != len(overlaps) {
			return model.ErrPrecondition
		}
		next := []model.Acceptance{}
		for _, a := range entries {
			for _, part := range model.Subtract(a.Valid, r.Valid) {
				piece := a
				piece.Valid = part
				next = append(next, piece)
			}
		}
		next = append(next, model.Acceptance{VersionID: v.ID, Valid: r.Valid, PolicyID: p.ID, AttestationIDs: attIDs, State: "accepted"})
		model.SortTimeline(next)
		if err := t.StageTimeline(ctx, rec.ID, next); err != nil {
			return err
		}
		*out = model.CommitResult{ID: v.ID, VersionID: v.ID, RecordID: rec.ID, ContentDigest: v.Digest, StreamRevision: rec.Revision + 1}
		return tx.SetRevision(ctx, rec.ID, out.StreamRevision)
	}, commitOnlyCheck(func() error { return currentBoundPrerequisites(commitPolicy, v.ID, v.Digest, commitIDs, verified) }))
}
func (s *Service) RejectFact(ctx context.Context, c model.Caller, r model.LifecycleChange) (model.CommitResult, error) {
	return s.change(ctx, c, r, "reject_fact", "can_approve")
}
func (s *Service) DisputeFact(ctx context.Context, c model.Caller, r model.LifecycleChange) (model.CommitResult, error) {
	return s.change(ctx, c, r, "dispute_fact", "can_dispute")
}
func (s *Service) ResolveDispute(ctx context.Context, c model.Caller, r model.LifecycleChange) (model.CommitResult, error) {
	return s.change(ctx, c, r, "resolve_dispute", "can_resolve")
}
func (s *Service) RetractFact(ctx context.Context, c model.Caller, r model.LifecycleChange) (model.CommitResult, error) {
	return s.change(ctx, c, r, "retract_fact", "can_resolve")
}
func (s *Service) change(ctx context.Context, c model.Caller, r model.LifecycleChange, op, permission string) (model.CommitResult, error) {
	if !model.ValidID(r.Reason) || (op != "reject_fact" && (r.Valid.From == nil || r.Valid.Validate() != nil)) {
		return model.CommitResult{}, model.ErrInvalid
	}
	return s.mutate(ctx, c, r.Scope, op, permission, r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := temporal(tx)
		if err != nil {
			return err
		}
		v, err := tx.Version(ctx, r.RecordID, r.VersionID)
		if err != nil {
			return err
		}
		if v.Proposal.Key.Scope != r.Scope {
			return model.ErrDenied
		}
		if op != "reject_fact" {
			definition, err := tx.Predicate(ctx, r.Scope, v.Proposal.Key.Predicate, v.Proposal.SchemaVersion)
			if err != nil {
				return err
			}
			if definition.ValidateFact(v.Proposal.Value, r.Valid) != nil {
				return model.ErrInvalid
			}
		}
		rec, err := tx.LockRecord(ctx, v.Proposal.Key)
		if err != nil {
			return err
		}
		if rec.Revision != r.ExpectedRevision {
			return &model.RevisionConflict{RecordID: rec.ID, ExpectedRevision: r.ExpectedRevision, CurrentRevision: rec.Revision}
		}
		if op == "reject_fact" {
			accepted, err := t.HasAcceptance(ctx, rec.ID, v.ID)
			if err != nil {
				return err
			}
			rejected, err := t.Rejected(ctx, rec.ID, v.ID, 0)
			if err != nil {
				return err
			}
			if accepted || rejected {
				return model.ErrPrecondition
			}
			if err := t.StageRejection(ctx, rec.ID, v.ID); err != nil {
				return err
			}
		} else {
			entries, err := tx.Acceptances(ctx, rec.ID, 0)
			if err != nil {
				return err
			}
			model.SortTimeline(entries)
			next := []model.Acceptance{}
			covered := r.Valid.From
			complete := false
			for _, a := range entries {
				if a.VersionID != v.ID || !a.Valid.Overlaps(r.Valid) {
					next = append(next, a)
					continue
				}
				allowed := (op == "dispute_fact" && a.State == "accepted") || (op == "resolve_dispute" && a.State == "disputed") || (op == "retract_fact" && (a.State == "accepted" || a.State == "disputed"))
				if !allowed {
					return model.ErrPrecondition
				}
				hit := model.Intersect(a.Valid, r.Valid)
				if covered == nil || !covered.Equal(*hit.From) {
					return model.ErrPrecondition
				}
				covered = hit.To
				if hit.To == nil || (r.Valid.To != nil && hit.To.Equal(*r.Valid.To)) {
					complete = true
				}
				for _, part := range model.Subtract(a.Valid, r.Valid) {
					piece := a
					piece.Valid = part
					next = append(next, piece)
				}
				a.Valid = hit
				switch op {
				case "dispute_fact":
					a.State = "disputed"
				case "resolve_dispute":
					a.State = "accepted"
				case "retract_fact":
					a.State = "retracted"
				}
				next = append(next, a)
			}
			if !complete {
				return model.ErrPrecondition
			}
			model.SortTimeline(next)
			if err := t.StageTimeline(ctx, rec.ID, next); err != nil {
				return err
			}
		}
		*out = model.CommitResult{ID: v.ID, RecordID: rec.ID, VersionID: v.ID, StreamRevision: rec.Revision + 1}
		return tx.SetRevision(ctx, rec.ID, out.StreamRevision)
	})
}

// resolve runs entirely inside one ledger view. Evidence bytes are verified by the
// caller outside this transaction before any value is disclosed or persisted.
func (s *Service) resolve(ctx context.Context, tx model.Tx, q model.FactQuery, revision int64, evaluatedAt time.Time, historical bool) (model.SnapshotItem, error) {
	item := model.SnapshotItem{Selector: q.Key, Resolution: model.FactResult{State: "missing", LedgerRevision: revision}}
	t, err := temporal(tx)
	if err != nil {
		return item, err
	}
	rec, err := tx.Record(ctx, model.Digest(q.Key))
	if errors.Is(err, model.ErrNotFound) {
		return item, nil
	}
	if err != nil {
		return item, err
	}
	stream, err := t.StreamRevisionAt(ctx, rec.ID, revision)
	if err != nil {
		return item, err
	}
	if stream == 0 {
		return item, nil
	}
	item.Resolution.RecordID = rec.ID
	item.Resolution.StreamRevision = stream
	entries, err := tx.Acceptances(ctx, rec.ID, revision)
	if err != nil {
		return item, err
	}
	for _, a := range entries {
		if !a.Valid.Contains(q.ValidAt) {
			continue
		}
		item.Resolution.VersionID = a.VersionID
		item.Valid = a.Valid
		item.PolicyID = a.PolicyID
		item.AttestationIDs = append([]string{}, a.AttestationIDs...)
		if a.State == "disputed" || a.State == "retracted" {
			item.Resolution.State = a.State
			return item, nil
		}
		v, err := tx.Version(ctx, rec.ID, a.VersionID)
		if err != nil {
			return item, err
		}
		item.SchemaVersion = v.Proposal.SchemaVersion
		p, err := tx.Policy(ctx, q.Key.Scope, a.PolicyID)
		if err != nil {
			return item, err
		}
		var as []model.Attestation
		if historical {
			as, err = t.HistoricalAttestations(ctx, v.ID, revision)
		} else {
			as, err = tx.Attestations(ctx, v.ID)
		}
		if err != nil {
			return item, err
		}
		// Only the prerequisites recorded with acceptance can keep it eligible.
		bound := []model.Attestation{}
		for _, att := range as {
			if slices.Contains(a.AttestationIDs, att.ID) {
				bound = append(bound, att)
				item.EvidenceIDs = append(item.EvidenceIDs, model.AttestationEvidence(att)...)
			}
		}
		if _, err := prerequisites(p, v, bound, evaluatedAt); err != nil {
			item.Resolution.State = "ineligible"
			return item, nil
		}
		for _, span := range v.Proposal.Sources {
			e, err := tx.Evidence(ctx, span.EvidenceID)
			if err != nil {
				return item, err
			}
			if !slices.Contains(item.EvidenceIDs, e.ID) {
				item.EvidenceIDs = append(item.EvidenceIDs, e.ID)
			}
			if !slices.Contains(item.SourceClasses, e.SourceClass) {
				item.SourceClasses = append(item.SourceClasses, e.SourceClass)
			}
		}
		item.Resolution.State = "resolved"
		if v.Proposal.Value.Kind == model.Unknown {
			item.Resolution.State = "unknown"
		}
		value := v.Proposal.Value
		item.Resolution.Value = &value
		return item, nil
	}
	versions, err := t.Versions(ctx, rec.ID, revision)
	if err != nil {
		return item, err
	}
	for _, v := range versions {
		rejected, err := t.Rejected(ctx, rec.ID, v.ID, revision)
		if err != nil {
			return item, err
		}
		if !rejected && (v.Proposal.Valid.From == nil || v.Proposal.Valid.Contains(q.ValidAt)) {
			item.Resolution.State = "unresolved"
			break
		}
	}
	return item, nil
}
func (s *Service) GetFact(ctx context.Context, c model.Caller, q model.FactQuery) (model.FactResult, error) {
	if !validSelector(q.Key) || q.ValidAt.IsZero() || q.KnowledgeRevision < 0 || (q.KnownAt != nil && (q.KnownAt.IsZero() || q.KnowledgeRevision != 0)) {
		return model.FactResult{}, model.ErrInvalid
	}
	if q.Purpose == "" {
		q.Purpose = "fact_read"
	}
	if _, err := s.authorize(ctx, c, q.Key.Scope, "can_read", model.Command{Purpose: q.Purpose}); err != nil {
		return model.FactResult{}, err
	}
	if q.KnowledgeRevision > 0 || q.KnownAt != nil {
		if _, err := s.authorize(ctx, c, q.Key.Scope, "can_read_history", model.Command{Purpose: q.Purpose}); err != nil {
			return model.FactResult{}, err
		}
	}
	var item model.SnapshotItem
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		revision, at, err := readSelection(ctx, tx, q.KnowledgeRevision, q.KnownAt)
		if err != nil {
			return err
		}
		if revision == 0 {
			item = model.SnapshotItem{Selector: q.Key, Resolution: model.FactResult{State: "missing"}}
			return nil
		}
		item, err = s.resolve(ctx, tx, q, revision, at, (q.KnowledgeRevision > 0 || q.KnownAt != nil))
		return err
	})
	if err != nil {
		return model.FactResult{}, err
	}
	if len(item.EvidenceIDs) > 0 {
		if _, err := s.loadEvidence(ctx, c, q.Key.Scope, item.EvidenceIDs); err != nil {
			if interrupted := evidenceInterruption(ctx, err); interrupted != nil {
				return model.FactResult{}, interrupted
			}
			item.Resolution.State = "ineligible"
			item.Resolution.Value = nil
		}
	}
	if _, err = s.snapshotAuthorization(ctx, c, []model.FactKey{q.Key}, model.Command{Purpose: q.Purpose}, q.KnowledgeRevision > 0 || q.KnownAt != nil, false); err != nil {
		return model.FactResult{}, err
	}
	return item.Resolution, nil
}
func validSelector(k model.FactKey) bool {
	return model.ValidID(k.Scope) && model.ValidID(k.Subject) && model.ValidID(k.Predicate) && (k.Discriminator == "" || model.ValidID(k.Discriminator))
}
func selection(ctx context.Context, tx model.Tx, requested int64) (int64, time.Time, error) {
	current, err := tx.Revision(ctx)
	if err != nil {
		return 0, time.Time{}, err
	}
	if requested == 0 {
		return current, time.Now().UTC(), nil
	}
	if requested < 0 || requested > current {
		return 0, time.Time{}, model.ErrInvalid
	}
	t, err := temporal(tx)
	if err != nil {
		return 0, time.Time{}, err
	}
	at, err := t.RevisionTime(ctx, requested)
	return requested, at, err
}
func (s *Service) GetFactHistory(ctx context.Context, c model.Caller, q model.HistoryQuery) (model.HistoryResult, error) {
	result := model.HistoryResult{}
	if _, err := s.authorize(ctx, c, q.Scope, "can_read_history", model.Command{Purpose: q.Purpose}); err != nil {
		return result, err
	}
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := temporal(tx)
		if err != nil {
			return err
		}
		rec, err := tx.Record(ctx, q.RecordID)
		if err != nil {
			return err
		}
		if rec.Kind == "memory" {
			return model.ErrPrecondition
		}
		if rec.Key.Scope != q.Scope {
			return model.ErrDenied
		}
		result.LedgerRevision, err = tx.Revision(ctx)
		if err != nil {
			return err
		}
		result.Events, err = t.History(ctx, q.RecordID)
		return err
	})
	return result, err
}
