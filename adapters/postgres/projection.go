package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"sort"
)

// ProjectionDocument resolves immutable content at the acceptance event's
// revision, even when a delayed delivery follows a later correction.
func (l *Ledger) ProjectionDocument(ctx context.Context, d model.Delivery, generation string) (*model.ProjectionDocument, error) {
	if !model.ValidID(generation) || !model.ValidID(d.Key.Tenant) {
		return nil, model.ErrInvalid
	}
	var result *model.ProjectionDocument
	err := l.Transact(ctx, d.Key.Tenant, func(tx model.Tx) error {
		t := tx.(*transaction)
		var e model.Event
		var payload []byte
		if err := t.tx.QueryRow(ctx, `SELECT payload FROM ledger_events WHERE tenant_id=$1 AND id=$2 AND ledger_revision=$3`, d.Key.Tenant, d.Event.ID, d.Event.LedgerRevision).Scan(&payload); err != nil {
			return err
		}
		if err := t.decode(payload, &e); err != nil {
			return err
		}
		switch e.Type {
		case "approve_fact", "accept_fact", "supersede_fact":
		default:
			return nil
		}
		if e.RecordID != d.Event.RecordID || e.VersionID != d.Event.VersionID || e.Scope != d.Event.Scope {
			return model.ErrPrecondition
		}
		v, err := t.Version(ctx, e.RecordID, e.VersionID)
		if err != nil {
			return err
		}
		accepted, err := t.Acceptances(ctx, e.RecordID, e.LedgerRevision)
		if err != nil {
			return err
		}
		found := false
		for _, a := range accepted {
			if a.VersionID == v.ID && a.State == "accepted" {
				found = true
			}
		}
		if !found {
			return model.ErrPrecondition
		}
		schema, err := t.Predicate(ctx, e.Scope, v.Proposal.Key.Predicate, v.Proposal.SchemaVersion)
		if err != nil {
			return err
		}
		classes := map[string]bool{}
		for _, span := range v.Proposal.Sources {
			e, err := t.Evidence(ctx, span.EvidenceID)
			if err != nil {
				return err
			}
			classes[e.SourceClass] = true
		}
		result = &model.ProjectionDocument{Tenant: d.Key.Tenant, Generation: generation, RecordID: v.RecordID, VersionID: v.ID, VersionDigest: v.Digest, PolicyID: v.PolicyID, Classification: schema.Classification, Key: v.Proposal.Key, Value: v.Proposal.Value, Valid: v.Proposal.Valid}
		for class := range classes {
			result.SourceClasses = append(result.SourceClasses, class)
		}
		sort.Strings(result.SourceClasses)
		return nil
	})
	return result, err
}
