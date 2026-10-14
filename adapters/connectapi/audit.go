package connectapi

import (
	"context"
	"encoding/json"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type AuditEngine interface {
	ListChanges(context.Context, model.Caller, model.ChangesQuery) (model.ChangesResult, error)
	StartAuditExport(context.Context, model.Caller, model.AuditExportRequest) (model.AuditExportResult, error)
	GetAuditExport(context.Context, model.Caller, model.AuditExportQuery) (model.AuditExportResult, error)
}
type auditService struct{ engine AuditEngine }

func (s *auditService) ListChanges(ctx context.Context, r *v1.ListChangesRequest) (*v1.ListChangesResponse, error) {
	out, err := s.engine.ListChanges(ctx, callerFrom(ctx), model.ChangesQuery{Scope: r.Scope, Purpose: r.Purpose, AfterRevision: r.AfterRevision, Limit: int(r.Limit)})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.ListChangesResponse{ThroughRevision: out.ThroughRevision, NextRevision: out.NextRevision}
	for _, e := range out.Events {
		response.Events = append(response.Events, c.event(e))
	}
	return response, c.err
}
func (s *auditService) StartAuditExport(ctx context.Context, r *v1.StartAuditExportRequest) (*v1.StartAuditExportResponse, error) {
	out, err := s.engine.StartAuditExport(ctx, callerFrom(ctx), model.AuditExportRequest{Command: command(r.Command), Scope: r.Scope, IncludeEvidence: r.IncludeEvidence})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.StartAuditExportResponse{Result: c.auditResult(out)}
	return response, c.err
}
func (s *auditService) GetAuditExport(ctx context.Context, r *v1.GetAuditExportRequest) (*v1.GetAuditExportResponse, error) {
	out, err := s.engine.GetAuditExport(ctx, callerFrom(ctx), model.AuditExportQuery{ID: r.Id, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetAuditExportResponse{Result: c.auditResult(out)}
	return response, c.err
}
func (c *responseConverter) auditResult(r model.AuditExportResult) *v1.AuditExportResult {
	out := &v1.AuditExportResult{Id: r.ID, State: r.State, CreatedRevision: r.CreatedRevision, CompletedRevision: r.CompletedRevision}
	if r.Artifact != (model.ObjectRef{}) {
		out.Artifact = objectToProto(r.Artifact)
	}
	if r.Bundle != nil {
		out.Bundle = c.auditBundle(*r.Bundle)
	}
	return out
}
func (c *responseConverter) auditBundle(b model.AuditBundle) *v1.AuditBundle {
	m := b.Manifest
	manifest := &v1.AuditManifest{SchemaVersion: m.SchemaVersion, Encoding: m.Encoding, Tenant: m.Tenant, Scope: m.Scope, ExportId: m.ExportID, ThroughRevision: m.ThroughRevision, EntryCount: int64(m.EntryCount)}
	for _, entry := range m.Entries {
		manifest.Entries = append(manifest.Entries, &v1.AuditManifestEntry{Kind: entry.Kind, Id: entry.ID, Digest: entry.Digest})
	}
	for _, gap := range m.Gaps {
		manifest.Gaps = append(manifest.Gaps, &v1.AuditGap{Kind: gap.Kind, Id: gap.ID, Reason: gap.Reason})
	}
	for _, a := range m.Authorization {
		manifest.Authorization = append(manifest.Authorization, c.authorization(a))
	}
	// This is the same canonical encoding signed by the engine, not protojson.
	canonical, err := json.Marshal(m)
	if err != nil {
		c.err = err
	}
	out := &v1.AuditBundle{Manifest: manifest, ManifestJson: canonical, Signature: &v1.AuditSignature{Algorithm: b.Signature.Algorithm, KeyId: b.Signature.KeyID, Signature: b.Signature.Bytes}}
	for _, entry := range b.Entries {
		out.Entries = append(out.Entries, &v1.AuditEntry{Kind: entry.Kind, Id: entry.ID, PayloadJson: entry.Payload})
	}
	return out
}
