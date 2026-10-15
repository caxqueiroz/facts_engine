package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/caxqueiroz/facts_engine/model"
	"sort"
	"time"
)

func (s *Service) exportAuthorization(ctx context.Context, c model.Caller, r model.AuditExportRequest) ([]model.AuthorizationDecision, error) {
	permissions := []string{"can_export_audit", "can_read_history", "can_read"}
	if r.IncludeEvidence {
		permissions = append(permissions, "can_read_evidence")
	}
	out := []model.AuthorizationDecision{}
	for _, permission := range permissions {
		d, err := s.authorize(ctx, c, r.Scope, permission, r.Command)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}
func (s *Service) StartAuditExport(ctx context.Context, c model.Caller, r model.AuditExportRequest) (model.AuditExportResult, error) {
	if s.audit.Signer == nil {
		return model.AuditExportResult{}, model.ErrPrecondition
	}
	if _, err := s.exportAuthorization(ctx, c, r); err != nil {
		return model.AuditExportResult{}, err
	}
	job := model.AuditExportJob{ID: id(), Caller: c, Request: r, State: "pending"}
	result, err := s.mutate(ctx, c, r.Scope, "start_audit_export", "can_export_audit", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := auditTx(tx)
		if err != nil {
			return err
		}
		out.ID = job.ID
		return t.StageAuditJob(ctx, job)
	})
	if err != nil {
		return model.AuditExportResult{}, err
	}
	return s.GetAuditExport(ctx, c, model.AuditExportQuery{ID: result.ID, Scope: r.Scope, Purpose: r.Purpose})
}
func (s *Service) auditJob(ctx context.Context, c model.Caller, q model.AuditExportQuery) (model.AuditExportJob, error) {
	if !model.ValidID(q.ID) {
		return model.AuditExportJob{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, q.Scope, "can_export_audit", model.Command{Purpose: q.Purpose}); err != nil {
		return model.AuditExportJob{}, err
	}
	var job model.AuditExportJob
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := auditTx(tx)
		if err != nil {
			return err
		}
		job, err = t.AuditJob(ctx, q.ID)
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		return job, model.ErrDenied
	}
	if err != nil {
		return job, err
	}
	if job.Request.Scope != q.Scope {
		return job, model.ErrDenied
	}
	request := job.Request
	request.Purpose = q.Purpose
	if _, err := s.exportAuthorization(ctx, c, request); err != nil {
		return job, err
	}
	return job, nil
}
func (s *Service) GetAuditExport(ctx context.Context, c model.Caller, q model.AuditExportQuery) (model.AuditExportResult, error) {
	if _, err := s.auditJob(ctx, c, q); err != nil {
		return model.AuditExportResult{}, err
	}
	var result model.AuditExportResult
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := auditTx(tx)
		if err != nil {
			return err
		}
		result, err = t.AuditExport(ctx, q.ID)
		return err
	})
	return result, err
}

// RunAuditExport processes one durable request. The trusted worker must supply
// the original requester identity; permissions are checked again at execution.
// Failed/cancelled attempts are retried after the lease expires. No worker starts implicitly.
func (s *Service) RunAuditExport(ctx context.Context, c model.Caller, q model.AuditExportQuery) (observationResult model.AuditExportResult, observationErr error) {
	ctx, observation := s.telemetry.Start(ctx, "export.audit")
	defer observation.End(&observationErr)
	if s.audit.Signer == nil {
		return model.AuditExportResult{}, model.ErrPrecondition
	}
	job, err := s.auditJob(ctx, c, q)
	if err != nil {
		return model.AuditExportResult{}, err
	}
	if job.Caller != c {
		return model.AuditExportResult{}, model.ErrDenied
	}
	if job.State == "completed" {
		return s.GetAuditExport(ctx, c, q)
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := auditTx(tx)
		if err != nil {
			return err
		}
		job, err = t.ClaimAuditJob(ctx, q.ID, model.AuditExportLease)
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		return model.AuditExportResult{}, model.ErrPrecondition
	}
	if err != nil {
		return model.AuditExportResult{}, err
	}
	var entries []model.AuditEntry
	var gaps []model.AuditGap
	var revision int64
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := auditTx(tx)
		if err != nil {
			return err
		}
		revision, err = tx.Revision(ctx)
		if err != nil {
			return err
		}
		entries, gaps, err = t.AuditEntries(ctx, q.Scope)
		return err
	})
	if err != nil {
		return model.AuditExportResult{}, err
	}
	entries, extra, err := s.prepareAuditEntries(ctx, c, job.Request, entries)
	if err != nil {
		return model.AuditExportResult{}, err
	}
	gaps = append(gaps, extra...)
	// Preserve the gap kind while distinguishing supported unreferenced lineage
	// disposal from the remaining copies embedded in referenced artifacts.
	gaps = append(gaps, model.AuditGap{Kind: "coordinated_derivative_disposal", Reason: "referenced_payload_disposal_not_implemented"})
	gaps = append(gaps, model.AuditGap{Kind: "independent_archive_chain", Reason: "archive_coverage_not_attached"})
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Kind == entries[j].Kind {
			return entries[i].ID < entries[j].ID
		}
		return entries[i].Kind < entries[j].Kind
	})
	sort.Slice(gaps, func(i, j int) bool {
		a, b := gaps[i], gaps[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return a.Reason < b.Reason
	})
	auth, err := s.exportAuthorization(ctx, c, job.Request)
	if err != nil {
		return model.AuditExportResult{}, err
	}
	manifest := model.AuditManifest{SchemaVersion: "facts-audit-v1", Encoding: "json-utf8-v1", Tenant: c.Tenant, Scope: q.Scope, ExportID: job.ID, ThroughRevision: revision, EntryCount: len(entries), Gaps: gaps, Authorization: auth}
	previous := ""
	size := 0
	for _, entry := range entries {
		key := entry.Kind + "\x00" + entry.ID
		if key <= previous {
			return model.AuditExportResult{}, model.ErrPrecondition
		}
		previous = key
		size += len(entry.Payload)
		manifest.Entries = append(manifest.Entries, model.AuditManifestEntry{Kind: entry.Kind, ID: entry.ID, Digest: model.BytesDigest(entry.Payload)})
	}
	if size > 64<<20 {
		return model.AuditExportResult{}, model.ErrPrecondition
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return model.AuditExportResult{}, err
	}
	signature, err := s.audit.Signer.Sign(ctx, payload)
	if err != nil {
		return model.AuditExportResult{}, model.ErrUnavailable
	}
	if signature.KeyID == "" || signature.Algorithm == "" || len(signature.Bytes) == 0 {
		return model.AuditExportResult{}, model.ErrPrecondition
	}
	bundle := model.AuditBundle{Manifest: manifest, Entries: entries, Signature: signature}
	command := model.Command{Key: fmt.Sprintf("%s-%d", job.ID, job.Fence), Purpose: job.Request.Purpose, CorrelationID: job.Request.CorrelationID}
	_, err = s.mutate(ctx, c, q.Scope, "complete_audit_export", "can_export_audit", command, struct {
		ID     string
		Fence  int64
		Digest string
	}{job.ID, job.Fence, model.Digest(bundle)}, func(tx model.Tx, out *model.CommitResult) error {
		for _, receipt := range auth {
			if time.Since(receipt.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		t, err := auditTx(tx)
		if err != nil {
			return err
		}
		out.ID = job.ID
		return t.StageAuditBundle(ctx, job, bundle)
	})
	if err != nil {
		return model.AuditExportResult{}, err
	}
	return s.GetAuditExport(ctx, c, q)
}
func (s *Service) prepareAuditEntries(ctx context.Context, c model.Caller, r model.AuditExportRequest, entries []model.AuditEntry) ([]model.AuditEntry, []model.AuditGap, error) {
	snapshots := map[string]bool{}
	disposedSnapshots := map[string]bool{}
	evidence := map[string]model.Evidence{}
	gaps := []model.AuditGap{}
	disposedEvidence := map[string]bool{}
	out := []model.AuditEntry{}
	for _, entry := range entries {
		switch entry.Kind {
		case "snapshot":
			var snap model.FactSnapshot
			if err := json.Unmarshal(entry.Payload, &snap); err != nil {
				return nil, nil, err
			}
			allowed := true
			for _, item := range snap.Items {
				if item.Selector.Scope != r.Scope {
					allowed = false
				}
			}
			snapshots[snap.ID] = allowed
			disposedSnapshots[snap.ID] = snap.PayloadDisposed
		case "evidence":
			var e model.Evidence
			if err := json.Unmarshal(entry.Payload, &e); err != nil {
				return nil, nil, err
			}
			evidence[e.ID] = e
		}
	}
	if r.IncludeEvidence {
		results := map[string]bool{}
		for _, entry := range entries {
			if entry.Kind != "extraction_result" {
				continue
			}
			var result model.ExtractionResult
			if err := json.Unmarshal(entry.Payload, &result); err != nil {
				return nil, nil, err
			}
			results[result.AttemptID] = true
			if !result.ResponseComplete {
				gaps = append(gaps, model.AuditGap{Kind: "extraction_response", ID: result.AttemptID, Reason: "complete_response_not_retained"})
			}
		}
		for _, entry := range entries {
			if entry.Kind == "extraction_attempt" && !results[entry.ID] {
				gaps = append(gaps, model.AuditGap{Kind: "extraction_response", ID: entry.ID, Reason: "response_not_retained_at_export_revision"})
			}
		}
	}
	size := 0
	for _, entry := range entries {
		include := true
		switch entry.Kind {
		case "memory_timeline":
			var entryTimeline model.Acceptance
			if err := json.Unmarshal(entry.Payload, &entryTimeline); err != nil {
				return nil, nil, err
			}
			for _, check := range entryTimeline.DependencyAuthorization {
				if check.Request.Scope != r.Scope {
					include = false
				}
			}
		case "memory_version":
			var memory model.MemoryVersion
			if err := json.Unmarshal(entry.Payload, &memory); err != nil {
				return nil, nil, err
			}
			if memory.PayloadDisposed && (memory.ID != entry.ID || memory.Proposer.Tenant != c.Tenant || model.Digest(memory) != model.Digest(model.MemoryVersionTombstone(memory))) {
				return nil, nil, model.ErrPrecondition
			}
			if !r.IncludeEvidence && !memory.PayloadDisposed {
				gaps = append(gaps, model.AuditGap{Kind: entry.Kind, ID: entry.ID, Reason: "memory_payload_withheld"})
				continue
			}
			for _, d := range memory.Proposal.Dependencies {
				if d.Scope != r.Scope {
					include = false
				}
			}
			if include && memory.PayloadDisposed {
				gaps = append(gaps, model.AuditGap{Kind: "memory_version_content", ID: entry.ID, Reason: "disposed_under_retention_policy"})
			}
		case "extraction_job", "extraction_attempt", "extraction_result":
			var artifact struct {
				RefreshScopes, ContentScopes []string
				PayloadDisposed              bool
				ID, AttemptID, Tenant, Scope string
				Request                      struct{ Scope string }
			}
			if err := json.Unmarshal(entry.Payload, &artifact); err != nil {
				return nil, nil, err
			}
			for _, scope := range append(artifact.RefreshScopes, artifact.ContentScopes...) {
				include = include && scope == r.Scope
			}
			if artifact.PayloadDisposed {
				id, scope := artifact.ID, artifact.Scope
				if entry.Kind == "extraction_job" {
					scope = artifact.Request.Scope
				}
				if entry.Kind == "extraction_result" {
					id = artifact.AttemptID
				}
				if id != entry.ID || artifact.Tenant != c.Tenant || !model.ValidExtractionTombstoneJSON(entry.Kind, entry.Payload) {
					return nil, nil, model.ErrPrecondition
				}
				include = include && scope == r.Scope
				if include {
					gaps = append(gaps, model.AuditGap{Kind: entry.Kind + "_content", ID: entry.ID, Reason: "disposed_under_retention_policy"})
				}
			}
			if !r.IncludeEvidence && !artifact.PayloadDisposed {
				gaps = append(gaps, model.AuditGap{Kind: entry.Kind, ID: entry.ID, Reason: "model_payloads_withheld"})
				continue
			}

		case "retention":
			var event model.RetentionEvent
			if err := json.Unmarshal(entry.Payload, &event); err != nil {
				return nil, nil, err
			}
			if event.SnapshotDisposal != nil {
				for _, b := range event.SnapshotDisposal.Bindings {
					include = include && b.Scope == r.Scope
				}
			}
			if event.ContextDisposal != nil {
				for _, b := range event.ContextDisposal.Bindings {
					include = include && b.Scope == r.Scope
				}
			}
			if event.CalculationDisposal != nil {
				for _, b := range event.CalculationDisposal.Bindings {
					include = include && b.Scope == r.Scope
				}
			}
			if event.ExtractionDisposal != nil {
				for _, b := range event.ExtractionDisposal.Bindings {
					include = include && b.Scope == r.Scope
				}
			}
			if event.DisposalBatch != nil {
				for _, scope := range event.DisposalBatch.Scopes() {
					include = include && scope == r.Scope
				}
			}
			if event.ProjectionCleanup != nil {
				for _, b := range event.ProjectionCleanup.Bindings {
					include = include && b.Scope == r.Scope
				}
			}
			if event.VersionDisposal != nil {
				for _, b := range event.VersionDisposal.Bindings {
					include = include && b.Scope == r.Scope
				}
			}
			if event.Kind == "dispose_evidence" {
				ids := []string{event.TargetID}
				if event.Disposal != nil {
					ids = append(ids, event.Disposal.DerivativeEvidenceIDs...)
				}
				for _, id := range ids {
					if !disposedEvidence[id] {
						gaps = append(gaps, model.AuditGap{Kind: "evidence_content", ID: id, Reason: "disposed_under_retention_policy"})
						disposedEvidence[id] = true
					}
				}
			}
		case "context":
			var m model.ContextManifest
			if err := json.Unmarshal(entry.Payload, &m); err != nil {
				return nil, nil, err
			}
			if len(m.Memories) > 0 && !m.PayloadDisposed && !r.IncludeEvidence {
				gaps = append(gaps, model.AuditGap{Kind: entry.Kind, ID: entry.ID, Reason: "memory_context_payload_withheld"})
				continue
			}
			for _, scope := range model.ContextScopes(m) {
				include = include && scope == r.Scope
			}
			if include && m.PayloadDisposed {
				gaps = append(gaps, model.AuditGap{Kind: "context_content", ID: m.ID, Reason: "disposed_under_retention_policy"})
			}
		case "decision":
			var d model.DecisionRecord
			if err := json.Unmarshal(entry.Payload, &d); err != nil {
				return nil, nil, err
			}
			if d.PayloadDisposed && !model.ValidDecisionTombstone(d, c.Tenant, entry.ID) {
				return nil, nil, model.ErrPrecondition
			}
			for _, scope := range d.Scopes {
				include = include && scope == r.Scope
			}

			if include && d.PayloadDisposed {
				gaps = append(gaps, model.AuditGap{Kind: "decision_content", ID: d.ID, Reason: "disposed_under_retention_policy"})
			}
		case "snapshot":
			include = snapshots[entry.ID]
			if include && disposedSnapshots[entry.ID] {
				gaps = append(gaps, model.AuditGap{Kind: "snapshot_content", ID: entry.ID, Reason: "disposed_under_retention_policy"})
			}
		case "calculation":
			var calc model.CalculationRecord
			if err := json.Unmarshal(entry.Payload, &calc); err != nil {
				return nil, nil, err
			}
			if calc.PayloadDisposed && !model.ValidCalculationTombstone(calc, c.Tenant, entry.ID) {
				return nil, nil, model.ErrPrecondition
			}
			include = snapshots[calc.Receipt.SnapshotID]
			for _, id := range calc.Receipt.ReferenceSnapshotIDs {
				include = include && snapshots[id]
			}
			_, input := evidence[calc.Receipt.Input.EvidenceID]
			_, output := evidence[calc.Receipt.Output.EvidenceID]
			include = include && input && output
			if include && calc.PayloadDisposed {
				gaps = append(gaps, model.AuditGap{Kind: "calculation_content", ID: calc.ID, Reason: "disposed_under_retention_policy"})
			}
		case "version":
			var v model.Version
			if err := json.Unmarshal(entry.Payload, &v); err != nil {
				return nil, nil, err
			}
			if v.PayloadDisposed {
				if v.ID != entry.ID || v.Proposer.Tenant != c.Tenant || model.Digest(v) != model.Digest(model.VersionTombstone(v)) {
					return nil, nil, model.ErrPrecondition
				}
				gaps = append(gaps, model.AuditGap{Kind: "version_content", ID: entry.ID, Reason: "disposed_under_retention_policy"})
			}
			if !r.IncludeEvidence && !v.PayloadDisposed {
				for i := range v.Proposal.Sources {
					v.Proposal.Sources[i].Quote = ""
				}
				entry.Payload, _ = json.Marshal(v)
				gaps = append(gaps, model.AuditGap{Kind: "version", ID: v.ID, Reason: "source_quotes_withheld"})
			}
		}
		if !include {
			gaps = append(gaps, model.AuditGap{Kind: entry.Kind, ID: entry.ID, Reason: "outside_export_scope"})
			continue
		}
		size += len(entry.Payload)
		if size > 64<<20 {
			return nil, nil, model.ErrPrecondition
		}
		out = append(out, entry)
	}
	ids := make([]string, 0, len(evidence))
	for id := range evidence {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		e := evidence[id]
		if !r.IncludeEvidence {
			gaps = append(gaps, model.AuditGap{Kind: "evidence_content", ID: id, Reason: "not_requested"})
			continue
		}
		b, mismatch, err := s.verifyEvidenceIntegrity(ctx, c.Tenant, e)
		if interrupted := evidenceDependencyFailure(ctx, err); interrupted != nil {
			return nil, nil, interrupted
		}
		if mismatch {
			s.telemetry.ExportIntegrityFailure(ctx, "evidence_content")
		}
		if err != nil {
			gaps = append(gaps, model.AuditGap{Kind: "evidence_content", ID: id, Reason: "unavailable_or_integrity_failure"})
			continue
		}
		payload, err := json.Marshal(b)
		if err != nil {
			return nil, nil, err
		}
		size += len(payload)
		if size > 64<<20 {
			return nil, nil, model.ErrPrecondition
		}
		out = append(out, model.AuditEntry{Kind: "evidence_content", ID: id, Payload: payload})
	}
	return out, gaps, nil
}
