package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"slices"
	"time"
)

// decisionReferences checks retained source identity without reevaluating fact eligibility.
func (s *Service) decisionReferences(ctx context.Context, c model.Caller, r model.DecisionReceipt, create bool) (model.DecisionRecord, map[model.VersionReference]string, error) {
	out := model.DecisionRecord{Tenant: c.Tenant, Caller: c, Receipt: r, Digest: model.DecisionDigest(r)}
	items := map[model.VersionReference]string{}
	scopes := map[string]bool{}
	if r.ContextID != "" {
		m, err := s.GetContextManifest(ctx, c, model.ContextQuery{ID: r.ContextID, Purpose: r.Purpose})
		if err != nil {
			return out, nil, err
		}
		out.SourceDigest = m.Digest
		for _, scope := range m.Request.Query.Scopes {
			scopes[scope] = true
		}
		for _, item := range m.Items {
			items[model.VersionReference{RecordID: item.Fact.RecordID, VersionID: item.Fact.VersionID}] = item.Key.Scope
		}
	} else {
		snap, err := s.GetFactSnapshot(ctx, c, model.SnapshotQuery{ID: r.SnapshotID, Purpose: r.Purpose})
		if err != nil {
			return out, nil, err
		}
		out.SourceDigest = snap.Digest
		for _, item := range snap.Items {
			scopes[item.Selector.Scope] = true
			if item.Resolution.State == "resolved" || item.Resolution.State == "unknown" {
				items[model.VersionReference{RecordID: item.Resolution.RecordID, VersionID: item.Resolution.VersionID}] = item.Selector.Scope
			}
		}
	}
	if !scopes[r.Scope] {
		return out, nil, model.ErrInvalid
	}
	selected := map[model.VersionReference]bool{}
	for _, item := range r.SelectedItems {
		if items[item] == "" || selected[item] {
			return out, nil, model.ErrInvalid
		}
		selected[item] = true
	}
	used := map[model.VersionReference]bool{}
	for _, item := range r.UsedItems {
		if !selected[item] || used[item] {
			return out, nil, model.ErrInvalid
		}
		used[item] = true
	}
	if r.CalculationID != "" {
		calc, err := s.GetCalculationReceipt(ctx, c, model.CalculationQuery{ID: r.CalculationID, Purpose: r.Purpose})
		if err != nil {
			return out, nil, err
		}
		// Calculation linkage is deliberately restricted to the same snapshot.
		if r.SnapshotID == "" || calc.Receipt.SnapshotID != r.SnapshotID {
			return out, nil, model.ErrInvalid
		}
		out.CalculationDigest = calc.Digest
		for _, sid := range append([]string{calc.Receipt.SnapshotID}, calc.Receipt.ReferenceSnapshotIDs...) {
			snap, err := s.GetFactSnapshot(ctx, c, model.SnapshotQuery{ID: sid, Purpose: r.Purpose})
			if err != nil {
				return out, nil, err
			}
			for _, item := range snap.Items {
				scopes[item.Selector.Scope] = true
			}
		}
		for _, ref := range []model.ArtifactReference{calc.Receipt.Input, calc.Receipt.Output} {
			var e model.Evidence
			err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error { var err error; e, err = tx.Evidence(ctx, ref.EvidenceID); return err })
			if err != nil {
				return out, nil, err
			}
			scopes[e.Scope] = true
		}
	}
	for _, ref := range model.DecisionArtifacts(r) {
		if !model.ValidID(ref.EvidenceID) || !validDigest(ref.Digest) {
			return out, nil, model.ErrInvalid
		}
		var e model.Evidence
		err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error { var err error; e, err = tx.Evidence(ctx, ref.EvidenceID); return err })
		if errors.Is(err, model.ErrNotFound) {
			err = model.ErrDenied
		}
		if err != nil {
			return out, nil, err
		}
		d, err := s.authorize(ctx, c, e.Scope, "can_read_evidence", r.Command)
		if err != nil {
			return out, nil, err
		}
		out.Authorization = append(out.Authorization, d)
		if e.Object.Digest != ref.Digest {
			return out, nil, model.ErrPrecondition
		}
		if create {
			if _, err = s.verify(ctx, c.Tenant, e); err != nil {
				return out, nil, err
			}
		}
		scopes[e.Scope] = true
	}
	for scope := range scopes {
		out.Scopes = append(out.Scopes, scope)
	}
	slices.Sort(out.Scopes)
	permission := "can_read"
	if create {
		permission = "can_record_decision"
	}
	for _, scope := range out.Scopes {
		d, err := s.authorize(ctx, c, scope, permission, r.Command)
		if err != nil {
			return out, nil, err
		}
		out.Authorization = append(out.Authorization, d)
	}
	return out, items, nil
}
func (s *Service) RecordDecision(ctx context.Context, c model.Caller, r model.DecisionReceipt) (model.DecisionRecord, error) {
	if !model.ValidID(r.Key) || !model.ValidID(r.Scope) || !model.ValidID(r.ApplicationID) || !model.ValidID(r.ExternalDecisionID) || !model.ValidID(r.MethodID) || !model.ValidID(r.MethodVersion) || !model.ValidID(r.TransformationVersion) || r.DecidedAt.IsZero() || !slices.Contains([]string{"succeeded", "failed"}, r.Outcome) || !slices.Contains([]string{"rule", "model"}, r.MethodKind) || len(r.SelectedItems) > 128 || len(r.UsedItems) > 128 || (r.ContextID == "") == (r.SnapshotID == "") {
		return model.DecisionRecord{}, model.ErrInvalid
	}
	if r.MethodKind == "model" && (r.ModelRequest == nil || r.ModelResponse == nil) || r.MethodKind == "rule" && (r.ModelRequest != nil || r.ModelResponse != nil) {
		return model.DecisionRecord{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, r.Scope, "can_record_decision", r.Command); err != nil {
		return model.DecisionRecord{}, err
	}
	r.DecidedAt = r.DecidedAt.UTC()
	record, items, err := s.decisionReferences(ctx, c, r, true)
	if err != nil {
		return model.DecisionRecord{}, err
	}
	record.ID = id()
	result, err := s.mutate(ctx, c, r.Scope, "record_decision", "can_record_decision", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := contextTx(tx)
		if err != nil {
			return err
		}
		existing, err := t.DecisionRun(ctx, r.ApplicationID, r.ExternalDecisionID)
		if err == nil {
			if existing.Digest != record.Digest || existing.Caller != c {
				return model.ErrConflict
			}
			out.ID = existing.ID
			out.ContentDigest = existing.Digest
			return nil
		}
		if !errors.Is(err, model.ErrNotFound) {
			return err
		}
		for _, d := range record.Authorization {
			if time.Since(d.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		if err = t.StageDecision(ctx, record, items); err != nil {
			return err
		}
		out.ID = record.ID
		out.ContentDigest = record.Digest
		return nil
	})
	if err != nil {
		return model.DecisionRecord{}, err
	}
	return s.GetDecisionReceipt(ctx, c, model.DecisionQuery{ID: result.ID, Purpose: r.Purpose})
}
func (s *Service) GetDecisionReceipt(ctx context.Context, c model.Caller, q model.DecisionQuery) (model.DecisionRecord, error) {
	if !model.ValidID(c.Tenant) || !model.ValidID(c.Principal) || !model.ValidID(q.ID) || q.Purpose == "" {
		return model.DecisionRecord{}, model.ErrInvalid
	}
	var record model.DecisionRecord
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := contextTx(tx)
		if err != nil {
			return err
		}
		record, err = t.Decision(ctx, q.ID)
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		err = model.ErrDenied
	}
	if err != nil {
		return model.DecisionRecord{}, err
	}
	r := record.Receipt
	r.Purpose = q.Purpose
	checked, _, err := s.decisionReferences(ctx, c, r, false)
	if err != nil {
		return model.DecisionRecord{}, err
	}
	if record.Tenant != c.Tenant || model.DecisionDigest(record.Receipt) != record.Digest || checked.SourceDigest != record.SourceDigest || checked.CalculationDigest != record.CalculationDigest || !slices.Equal(checked.Scopes, record.Scopes) {
		return model.DecisionRecord{}, model.ErrPrecondition
	}
	return record, nil
}
