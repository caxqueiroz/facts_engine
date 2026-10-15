package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"io"
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
		result = nil
		t := tx.(*transaction)
		var e model.Event
		payload, err := ledgerdb.New(t.tx).GetEventAtRevision(ctx, ledgerdb.GetEventAtRevisionParams{TenantID: d.Key.Tenant, ID: d.Event.ID, LedgerRevision: d.Event.LedgerRevision})
		if err != nil {
			return err
		}
		if err := t.decode(payload, &e); err != nil {
			return err
		}
		switch e.Type {
		case "approve_fact", "accept_fact", "supersede_fact", "approve_memory", "accept_memory", "supersede_memory":
		default:
			return nil
		}
		if e.RecordID != d.Event.RecordID || e.VersionID != d.Event.VersionID || e.Scope != d.Event.Scope {
			return model.ErrPrecondition
		}
		if e.RecordKind == "memory" {
			var err error
			result, err = t.memoryProjection(ctx, e, generation)
			return err
		}
		v, err := t.retainedVersion(ctx, e.RecordID, e.VersionID)
		if err != nil {
			return err
		}
		if v.ID != e.VersionID || v.RecordID != e.RecordID || v.Proposal.Key.Scope != e.Scope || v.Proposer.Tenant != t.tenant {
			return model.ErrPrecondition
		}
		disposal, err := ledgerdb.New(t.tx).GetFactVersionDisposalPayload(ctx, ledgerdb.GetFactVersionDisposalPayloadParams{TenantID: t.tenant, VersionID: v.ID})
		if err != nil {
			return err
		}
		if (len(disposal) > 0) != v.PayloadDisposed {
			return model.ErrPrecondition
		}
		if len(disposal) > 0 {
			if t.decodeProjectionTombstone(disposal, &v) != nil || model.Digest(v) != model.Digest(model.VersionTombstone(v)) {
				return model.ErrPrecondition
			}
			return nil
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

func (t *transaction) memoryProjection(ctx context.Context, e model.Event, generation string) (*model.ProjectionDocument, error) {
	v, err := t.retainedMemoryVersion(ctx, e.RecordID, e.VersionID)
	if err != nil {
		return nil, err
	}
	if v.ID != e.VersionID || v.RecordID != e.RecordID || v.Proposer.Tenant != t.tenant || v.Proposal.Key.Scope != e.Scope {
		return nil, model.ErrPrecondition
	}
	disposal, err := ledgerdb.New(t.tx).GetMemoryVersionDisposalPayload(ctx, ledgerdb.GetMemoryVersionDisposalPayloadParams{TenantID: t.tenant, VersionID: v.ID})
	if err != nil {
		return nil, err
	}
	if (len(disposal) > 0) != v.PayloadDisposed {
		return nil, model.ErrPrecondition
	}
	if len(disposal) > 0 {
		if t.decodeProjectionTombstone(disposal, &v) != nil || model.Digest(v) != model.Digest(model.MemoryVersionTombstone(v)) {
			return nil, model.ErrPrecondition
		}
		return nil, nil
	}
	if v.Proposal.Key.Scope != e.Scope || model.MemoryDigest(v.Proposal) != v.Digest {
		return nil, model.ErrPrecondition
	}
	entries, err := t.MemoryTimeline(ctx, e.RecordID, e.LedgerRevision)
	if err != nil {
		return nil, err
	}
	found := false
	for _, entry := range entries {
		if entry.VersionID == v.ID && entry.State == "accepted" {
			found = true
		}
	}
	if !found {
		return nil, model.ErrPrecondition
	}
	definition, err := t.MemoryType(ctx, e.Scope, v.Proposal.Subtype, v.Proposal.SchemaVersion)
	if err != nil {
		return nil, err
	}
	doc := &model.ProjectionDocument{Tenant: t.tenant, Generation: generation, RecordID: v.RecordID, VersionID: v.ID, VersionDigest: v.Digest, PolicyID: v.PolicyID, Classification: definition.Classification, Valid: v.Proposal.Valid, Memory: &model.MemoryProjection{Key: v.Proposal.Key, Subtype: v.Proposal.Subtype, SchemaVersion: v.Proposal.SchemaVersion, Body: v.Proposal.Body, Dependencies: v.Proposal.Dependencies}}
	classes := map[string]bool{}
	for _, span := range v.Proposal.Sources {
		evidence, err := t.Evidence(ctx, span.EvidenceID)
		if err != nil {
			return nil, err
		}
		classes[evidence.SourceClass] = true
	}
	for class := range classes {
		doc.SourceClasses = append(doc.SourceClasses, class)
	}
	sort.Strings(doc.SourceClasses)
	return doc, nil
}

// Disposal metadata must not conceal retained content in unrecognized fields.
func (t *transaction) decodeProjectionTombstone(sealed []byte, v any) error {
	raw, err := t.cipher.Open(t.tenant, sealed)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(v); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return model.ErrPrecondition
	}
	return nil
}
