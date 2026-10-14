package services

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"slices"
	"time"
)

func auditTx(tx model.Tx) (model.AuditTx, error) {
	t, ok := tx.(model.AuditTx)
	if !ok {
		return nil, model.ErrPrecondition
	}
	return t, nil
}
func validDigest(d string) bool {
	b, err := hex.DecodeString(d)
	return err == nil && len(b) == 32 && d == hex.EncodeToString(b)
}
func (s *Service) calculationReferences(ctx context.Context, c model.Caller, r model.CalculationReceipt) ([]model.FactSnapshot, []model.AuthorizationDecision, error) {
	snapshots := []model.FactSnapshot{}
	receipts := []model.AuthorizationDecision{}
	ids := append([]string{r.SnapshotID}, r.ReferenceSnapshotIDs...)
	seen := map[string]bool{}
	for _, id := range ids {
		if !model.ValidID(id) || seen[id] {
			return nil, nil, model.ErrInvalid
		}
		seen[id] = true
		snap, err := s.GetFactSnapshot(ctx, c, model.SnapshotQuery{ID: id, Purpose: r.Purpose})
		if err != nil {
			return nil, nil, err
		}
		snapshots = append(snapshots, snap)
		for _, item := range snap.Items {
			d, err := s.authorize(ctx, c, item.Selector.Scope, "can_record_calculation", r.Command)
			if err != nil {
				return nil, nil, err
			}
			receipts = append(receipts, d)
		}
	}
	if !slices.ContainsFunc(snapshots[0].Items, func(item model.SnapshotItem) bool { return item.Selector.Scope == r.Scope }) {
		return nil, nil, model.ErrInvalid
	}
	for _, ref := range []model.ArtifactReference{r.Input, r.Output} {
		if !model.ValidID(ref.EvidenceID) || !validDigest(ref.Digest) {
			return nil, nil, model.ErrInvalid
		}
		var e model.Evidence
		err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error { var err error; e, err = tx.Evidence(ctx, ref.EvidenceID); return err })
		if err != nil {
			return nil, nil, err
		}
		d, err := s.authorize(ctx, c, e.Scope, "can_read_evidence", r.Command)
		if err != nil {
			return nil, nil, err
		}
		if e.Object.Digest != ref.Digest {
			return nil, nil, model.ErrPrecondition
		}
		receipts = append(receipts, d)
		if _, err := s.verify(ctx, c.Tenant, e); err != nil {
			return nil, nil, err
		}
	}
	return snapshots, receipts, nil
}
func (s *Service) RecordCalculation(ctx context.Context, c model.Caller, r model.CalculationReceipt) (model.CalculationRecord, error) {
	if !model.ValidID(r.Key) || !model.ValidID(r.Scope) || !model.ValidID(r.ApplicationID) || !model.ValidID(r.ExternalRunID) || !model.ValidID(r.MethodID) || !model.ValidID(r.MethodVersion) || !model.ValidID(r.NormalizationVersion) || !validDigest(r.ExecutableDigest) || !validDigest(r.ConfigurationDigest) || r.StartedAt.IsZero() || r.CompletedAt.Before(r.StartedAt) || !slices.Contains([]string{"succeeded", "failed"}, r.Outcome) || len(r.ReferenceSnapshotIDs) > 32 {
		return model.CalculationRecord{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, r.Scope, "can_record_calculation", r.Command); err != nil {
		return model.CalculationRecord{}, err
	}
	r.StartedAt = r.StartedAt.UTC()
	r.CompletedAt = r.CompletedAt.UTC()
	snapshots, receipts, err := s.calculationReferences(ctx, c, r)
	if err != nil {
		return model.CalculationRecord{}, err
	}
	record := model.CalculationRecord{ID: id(), Tenant: c.Tenant, Caller: c, Receipt: r, Digest: model.CalculationDigest(r), SnapshotDigest: snapshots[0].Digest, Authorization: receipts}
	// The external run has a second idempotency identity independent of command keys.
	// Its lock and lookup happen inside the same transaction as the command receipt.
	result, err := s.mutate(ctx, c, r.Scope, "record_calculation", "can_record_calculation", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := auditTx(tx)
		if err != nil {
			return err
		}
		existing, err := t.CalculationRun(ctx, r.ApplicationID, r.ExternalRunID)
		if err == nil {
			if existing.Digest != record.Digest {
				return model.ErrConflict
			}
			out.ID = existing.ID
			out.ContentDigest = existing.Digest
			return nil
		}
		if !errors.Is(err, model.ErrNotFound) {
			return err
		}
		for _, receipt := range receipts {
			if time.Since(receipt.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		if err := t.StageCalculation(ctx, record, snapshots); err != nil {
			return err
		}
		out.ID = record.ID
		out.ContentDigest = record.Digest
		return nil
	})
	if err != nil {
		return model.CalculationRecord{}, err
	}
	return s.GetCalculationReceipt(ctx, c, model.CalculationQuery{ID: result.ID, Purpose: r.Purpose})
}
func (s *Service) GetCalculationReceipt(ctx context.Context, c model.Caller, q model.CalculationQuery) (model.CalculationRecord, error) {
	if !model.ValidID(c.Tenant) || !model.ValidID(c.Principal) || !model.ValidID(q.ID) || q.Purpose == "" {
		return model.CalculationRecord{}, model.ErrInvalid
	}
	var record model.CalculationRecord
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := auditTx(tx)
		if err != nil {
			return err
		}
		record, err = t.Calculation(ctx, q.ID)
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		return model.CalculationRecord{}, model.ErrDenied
	}
	if err != nil {
		return model.CalculationRecord{}, err
	}
	if record.Tenant != c.Tenant || model.CalculationDigest(record.Receipt) != record.Digest {
		return model.CalculationRecord{}, model.ErrPrecondition
	}
	for _, id := range append([]string{record.Receipt.SnapshotID}, record.Receipt.ReferenceSnapshotIDs...) {
		if _, err := s.GetFactSnapshot(ctx, c, model.SnapshotQuery{ID: id, Purpose: q.Purpose}); err != nil {
			return model.CalculationRecord{}, err
		}
	}
	if _, err := s.authorize(ctx, c, record.Receipt.Scope, "can_read", model.Command{Purpose: q.Purpose}); err != nil {
		return model.CalculationRecord{}, err
	}
	for _, ref := range []model.ArtifactReference{record.Receipt.Input, record.Receipt.Output} {
		var evidence model.Evidence
		err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
			var err error
			evidence, err = tx.Evidence(ctx, ref.EvidenceID)
			return err
		})
		if errors.Is(err, model.ErrNotFound) {
			return model.CalculationRecord{}, model.ErrDenied
		}
		if err != nil {
			return model.CalculationRecord{}, err
		}
		if _, err := s.authorize(ctx, c, evidence.Scope, "can_read_evidence", model.Command{Purpose: q.Purpose}); err != nil {
			return model.CalculationRecord{}, err
		}
	}
	return record, nil
}
