package postgres

import (
	"context"
	"math"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) ContextManifest(ctx context.Context, id string) (model.ContextManifest, error) {
	var m model.ContextManifest
	payload, err := ledgerdb.New(t.tx).GetContextPayload(ctx, ledgerdb.GetContextPayloadParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return m, translate(err)
	}
	if len(payload) == 0 {
		return m, model.ErrPrecondition
	}
	err = t.decode(payload, &m)
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
	payload, err := ledgerdb.New(t.tx).GetDecisionPayload(ctx, ledgerdb.GetDecisionPayloadParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return r, translate(err)
	}
	if len(payload) == 0 {
		return r, model.ErrPrecondition
	}
	err = t.decode(payload, &r)
	return r, err
}
func (t *transaction) DecisionRun(ctx context.Context, application, run string) (model.DecisionRecord, error) {
	lock := model.Digest([]string{t.tenant, "decision", application, run})
	if err := ledgerdb.New(t.tx).LockDecisionRun(ctx, lock); err != nil {
		return model.DecisionRecord{}, err
	}
	var r model.DecisionRecord
	payload, err := ledgerdb.New(t.tx).GetDecisionRunPayload(ctx, ledgerdb.GetDecisionRunPayloadParams{TenantID: t.tenant, ApplicationID: application, ExternalDecisionID: run})
	if err != nil {
		return r, translate(err)
	}
	if len(payload) == 0 {
		return r, model.ErrPrecondition
	}
	err = t.decode(payload, &r)
	return r, err
}
func (t *transaction) StageDecision(ctx context.Context, r model.DecisionRecord, scopes map[model.VersionReference]string) error {
	if r.Receipt.CalculationID != "" {
		if err := t.guardCalculationContent(ctx, r.Receipt.CalculationID); err != nil {
			return err
		}
	}
	if r.Receipt.ContextID != "" {
		if err := t.guardContextContent(ctx, r.Receipt.ContextID); err != nil {
			return err
		}
	}
	if r.Receipt.SnapshotID != "" {
		if err := t.guardSnapshotContent(ctx, r.Receipt.SnapshotID); err != nil {
			return err
		}
	}
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
func (t *transaction) flushContext(ctx context.Context, e model.Event) error {
	if t.pendingContext != nil {
		m := *t.pendingContext
		m.CreationRevision = e.LedgerRevision
		b, err := t.encode(m)
		if err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertContextManifest(ctx, ledgerdb.InsertContextManifestParams{TenantID: t.tenant, ID: m.ID, ScopeID: m.Scope, SelectionRevision: m.SelectionRevision, LedgerRevision: e.LedgerRevision}); err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertContextPayload(ctx, ledgerdb.InsertContextPayloadParams{TenantID: t.tenant, ContextID: m.ID, Payload: b}); err != nil {
			return err
		}
		for _, scope := range model.ContextScopes(m) {
			if err = ledgerdb.New(t.tx).InsertContextScope(ctx, ledgerdb.InsertContextScopeParams{TenantID: t.tenant, ContextID: m.ID, ScopeID: scope}); err != nil {
				return err
			}
		}
		for ordinal, item := range m.Memories {
			if ordinal > math.MaxInt32 {
				return model.ErrInvalid
			}
			if err = ledgerdb.New(t.tx).InsertContextMemoryItem(ctx, ledgerdb.InsertContextMemoryItemParams{TenantID: t.tenant, ContextID: m.ID, Ordinal: int32(ordinal), RecordID: item.Memory.RecordID, VersionID: item.Memory.VersionID, ScopeID: item.Key.Scope}); err != nil {
				return err
			}
		}
		for ordinal, item := range m.Items {
			if ordinal > math.MaxInt32 {
				return model.ErrInvalid
			}
			if err = ledgerdb.New(t.tx).InsertContextFactItem(ctx, ledgerdb.InsertContextFactItemParams{TenantID: t.tenant, ContextID: m.ID, Ordinal: int32(ordinal), RecordID: item.Fact.RecordID, VersionID: item.Fact.VersionID, ScopeID: item.Key.Scope}); err != nil {
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
		if err = ledgerdb.New(t.tx).InsertDecisionHeader(ctx, ledgerdb.InsertDecisionHeaderParams{TenantID: t.tenant, ID: r.ID, ScopeID: r.Receipt.Scope, ApplicationID: r.Receipt.ApplicationID, ExternalDecisionID: r.Receipt.ExternalDecisionID, RequestDigest: r.Digest, ContextID: nullableID(r.Receipt.ContextID), SnapshotID: nullableID(r.Receipt.SnapshotID), CalculationID: nullableID(r.Receipt.CalculationID), LedgerRevision: e.LedgerRevision}); err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertDecisionPayload(ctx, ledgerdb.InsertDecisionPayloadParams{TenantID: t.tenant, DecisionID: r.ID, Payload: b}); err != nil {
			return err
		}
		for _, scope := range r.Scopes {
			if err = ledgerdb.New(t.tx).InsertDecisionScope(ctx, ledgerdb.InsertDecisionScopeParams{TenantID: t.tenant, DecisionID: r.ID, ScopeID: scope}); err != nil {
				return err
			}
		}
		for _, group := range []struct {
			role  string
			items []model.VersionReference
		}{{"selected", r.Receipt.SelectedItems}, {"used", r.Receipt.UsedItems}} {
			for ordinal, item := range group.items {
				if ordinal > math.MaxInt32 {
					return model.ErrInvalid
				}
				scope, ok := t.decisionItemScopes[item]
				if !ok {
					return model.ErrInvalid
				}
				record, err := t.Record(ctx, item.RecordID)
				if err != nil {
					return err
				}
				if record.Kind == "memory" {
					if err = ledgerdb.New(t.tx).InsertDecisionMemoryItem(ctx, ledgerdb.InsertDecisionMemoryItemParams{TenantID: t.tenant, DecisionID: r.ID, Role: group.role, Ordinal: int32(ordinal), RecordID: item.RecordID, VersionID: item.VersionID, ScopeID: scope}); err != nil {
						return err
					}
					continue
				}
				if err = ledgerdb.New(t.tx).InsertDecisionFactItem(ctx, ledgerdb.InsertDecisionFactItemParams{TenantID: t.tenant, DecisionID: r.ID, Role: group.role, Ordinal: int32(ordinal), RecordID: item.RecordID, VersionID: item.VersionID, ScopeID: scope}); err != nil {
					return err
				}
			}
		}
		for role, ref := range model.DecisionArtifacts(r.Receipt) {
			if err = ledgerdb.New(t.tx).InsertDecisionArtifact(ctx, ledgerdb.InsertDecisionArtifactParams{TenantID: t.tenant, DecisionID: r.ID, Role: role, EvidenceID: ref.EvidenceID}); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ model.ContextTx = (*transaction)(nil)
