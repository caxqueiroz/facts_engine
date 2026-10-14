package connectapi

import (
	"errors"
	"time"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Conversion errors fail the complete response, never a partial result set.
type responseConverter struct{ err error }

func (c *responseConverter) timestamp(t time.Time) *timestamppb.Timestamp {
	out := timestamppb.New(t)
	if out.CheckValid() != nil {
		c.err = errors.New("engine timestamp outside protobuf range")
	}
	return out
}
func (c *responseConverter) interval(i model.Interval) *v1.Interval {
	out := &v1.Interval{}
	if i.From != nil {
		out.From = c.timestamp(*i.From)
	}
	if i.To != nil {
		out.To = c.timestamp(*i.To)
	}
	return out
}
func callerToProto(i model.Caller) *v1.Caller {
	return &v1.Caller{Tenant: i.Tenant, Principal: i.Principal, Initiator: i.Initiator, Service: i.Service}
}
func (c *responseConverter) authorization(a model.AuthorizationDecision) *v1.AuthorizationDecision {
	r := a.Request
	return &v1.AuthorizationDecision{Allowed: a.Allowed, ModelId: a.ModelID, ContextDigest: a.ContextDigest, Consistency: a.Consistency, CheckedAt: c.timestamp(a.CheckedAt), Request: &v1.AuthorizationRequest{Caller: callerToProto(r.Caller), Scope: r.Scope, Permission: r.Permission, Purpose: r.Purpose, CorrelationId: r.CorrelationID}}
}
func keyToProto(k model.FactKey) *v1.FactKey {
	return &v1.FactKey{Scope: k.Scope, Subject: k.Subject, Predicate: k.Predicate, Discriminator: k.Discriminator}
}
func (c *responseConverter) factResult(r model.FactResult) *v1.FactResult {
	out := &v1.FactResult{State: r.State, RecordId: r.RecordID, VersionId: r.VersionID, LedgerRevision: r.LedgerRevision, StreamRevision: r.StreamRevision}
	if r.Value != nil {
		value, err := valueToProto(*r.Value)
		if err != nil {
			c.err = err
		}
		out.Value = value
	}
	return out
}
func (c *responseConverter) snapshotItem(i model.SnapshotItem) *v1.SnapshotItem {
	return &v1.SnapshotItem{Selector: keyToProto(i.Selector), Resolution: c.factResult(i.Resolution), SchemaVersion: i.SchemaVersion, PolicyId: i.PolicyID, Valid: c.interval(i.Valid), SourceClasses: i.SourceClasses, EvidenceIds: i.EvidenceIDs, AttestationIds: i.AttestationIDs}
}
func (c *responseConverter) snapshot(s model.FactSnapshot) *v1.FactSnapshot {
	out := &v1.FactSnapshot{Id: s.ID, Tenant: s.Tenant, Caller: callerToProto(s.Caller), Purpose: s.Purpose, ValidAt: c.timestamp(s.ValidAt), EvaluatedAt: c.timestamp(s.EvaluatedAt), SelectionRevision: s.SelectionRevision, CreationRevision: s.CreationRevision, CanonicalVersion: s.CanonicalVersion, Digest: s.Digest}
	for _, item := range s.Items {
		out.Items = append(out.Items, c.snapshotItem(item))
	}
	for _, authorization := range s.Authorization {
		out.Authorization = append(out.Authorization, c.authorization(authorization))
	}
	return out
}
func (c *responseConverter) event(e model.Event) *v1.Event {
	out := &v1.Event{Id: e.ID, Type: e.Type, Scope: e.Scope, RecordId: e.RecordID, VersionId: e.VersionID, RecordKind: e.RecordKind, StreamRevision: e.StreamRevision, LedgerRevision: e.LedgerRevision, Caller: callerToProto(e.Caller), Command: &v1.Command{Key: e.Command.Key, Purpose: e.Command.Purpose, CorrelationId: e.Command.CorrelationID}, Authorization: c.authorization(e.Authorization), RecordedAt: c.timestamp(e.RecordedAt), EntityId: e.EntityID, Reason: e.Reason, ReplacedVersionIds: e.ReplacedVersionIDs, DefinitionPredicate: e.DefinitionPredicate}
	if e.Affected != nil {
		out.Affected = c.interval(*e.Affected)
	}
	if e.RefreshSweep != nil {
		r := e.RefreshSweep
		out.RefreshSweep = &v1.RefreshSweep{Stream: r.Stream, ProfileId: r.ProfileID, ProfileDigest: r.ProfileDigest, ScheduledFor: c.timestamp(r.ScheduledFor), IntervalNanoseconds: int64(r.Interval)}
	}
	return out
}
