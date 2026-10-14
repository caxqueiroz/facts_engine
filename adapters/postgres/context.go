package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) ContextManifest(ctx context.Context, id string) (model.ContextManifest, error) {
	var m model.ContextManifest
	err := t.read(ctx, &m, `SELECT payload FROM context_manifests WHERE tenant_id=$1 AND id=$2`, t.tenant, id)
	return m, err
}
func (t *transaction) StageContext(_ context.Context, m model.ContextManifest) error {
	if m.Tenant != t.tenant {
		return model.ErrInvalid
	}
	t.pendingContext = &m
	return nil
}
func (t *transaction) Decision(ctx context.Context, id string) (model.DecisionRecord, error) {
	var r model.DecisionRecord
	err := t.read(ctx, &r, `SELECT payload FROM decision_receipts WHERE tenant_id=$1 AND id=$2`, t.tenant, id)
	return r, err
}
func (t *transaction) DecisionRun(ctx context.Context, application, run string) (model.DecisionRecord, error) {
	lock := model.Digest([]string{t.tenant, "decision", application, run})
	if _, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lock); err != nil {
		return model.DecisionRecord{}, err
	}
	var r model.DecisionRecord
	err := t.read(ctx, &r, `SELECT payload FROM decision_receipts WHERE tenant_id=$1 AND application_id=$2 AND external_decision_id=$3`, t.tenant, application, run)
	return r, err
}
func (t *transaction) StageDecision(ctx context.Context, r model.DecisionRecord, scopes map[model.VersionReference]string) error {
	for _, ref := range model.DecisionArtifacts(r.Receipt) {
		if err := t.guardEvidenceReference(ctx, ref.EvidenceID); err != nil {
			return err
		}
	}
	if r.Tenant != t.tenant {
		return model.ErrInvalid
	}
	t.pendingDecision = &r
	t.decisionItemScopes = scopes
	return nil
}
func nullableID(id string) any {
	if id == "" {
		return nil
	}
	return id
}
func (t *transaction) flushContext(ctx context.Context, e model.Event) error {
	if t.pendingContext != nil {
		m := *t.pendingContext
		m.CreationRevision = e.LedgerRevision
		b, err := t.encode(m)
		if err != nil {
			return err
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO context_manifests VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, m.ID, m.Scope, m.SelectionRevision, e.LedgerRevision, b); err != nil {
			return err
		}
		for _, scope := range model.ContextScopes(m) {
			if _, err = t.tx.Exec(ctx, `INSERT INTO context_scopes VALUES($1,$2,$3)`, t.tenant, m.ID, scope); err != nil {
				return err
			}
		}
		for ordinal, item := range m.Memories {
			if _, err = t.tx.Exec(ctx, `INSERT INTO context_memory_items VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, m.ID, ordinal, item.Memory.RecordID, item.Memory.VersionID, item.Key.Scope); err != nil {
				return err
			}
		}
		for ordinal, item := range m.Items {
			if _, err = t.tx.Exec(ctx, `INSERT INTO context_items VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, m.ID, ordinal, item.Fact.RecordID, item.Fact.VersionID, item.Key.Scope); err != nil {
				return err
			}
		}
	}
	if t.pendingDecision != nil {
		r := *t.pendingDecision
		r.LedgerRevision = e.LedgerRevision
		b, err := t.encode(r)
		if err != nil {
			return err
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO decision_receipts VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, t.tenant, r.ID, r.Receipt.Scope, r.Receipt.ApplicationID, r.Receipt.ExternalDecisionID, r.Digest, nullableID(r.Receipt.ContextID), nullableID(r.Receipt.SnapshotID), nullableID(r.Receipt.CalculationID), e.LedgerRevision, b); err != nil {
			return err
		}
		for _, scope := range r.Scopes {
			if _, err = t.tx.Exec(ctx, `INSERT INTO decision_scopes VALUES($1,$2,$3)`, t.tenant, r.ID, scope); err != nil {
				return err
			}
		}
		for _, group := range []struct {
			role  string
			items []model.VersionReference
		}{{"selected", r.Receipt.SelectedItems}, {"used", r.Receipt.UsedItems}} {
			for ordinal, item := range group.items {
				scope, ok := t.decisionItemScopes[item]
				if !ok {
					return model.ErrInvalid
				}
				record, err := t.Record(ctx, item.RecordID)
				if err != nil {
					return err
				}
				if record.Kind == "memory" {
					if _, err = t.tx.Exec(ctx, `INSERT INTO decision_memory_items VALUES($1,$2,$3,$4,$5,$6,$7)`, t.tenant, r.ID, group.role, ordinal, item.RecordID, item.VersionID, scope); err != nil {
						return err
					}
					continue
				}
				if _, err = t.tx.Exec(ctx, `INSERT INTO decision_items VALUES($1,$2,$3,$4,$5,$6,$7)`, t.tenant, r.ID, group.role, ordinal, item.RecordID, item.VersionID, scope); err != nil {
					return err
				}
			}
		}
		for role, ref := range model.DecisionArtifacts(r.Receipt) {
			if _, err = t.tx.Exec(ctx, `INSERT INTO decision_artifacts VALUES($1,$2,$3,$4)`, t.tenant, r.ID, role, ref.EvidenceID); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ model.ContextTx = (*transaction)(nil)
