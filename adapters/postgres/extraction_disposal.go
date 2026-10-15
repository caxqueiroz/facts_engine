package postgres

import (
	"context"
	"errors"
	"sort"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// DisposeExtraction removes only the retained job and provider artifacts. Source
// records and live derived proposals are not implicitly selected for disposal.
func (t *transaction) DisposeExtraction(ctx context.Context, r model.ExtractionDisposalRequest) error {
	return t.disposeExtraction(ctx, r, nil, nil)
}

// Selected versions are supplied only by DisposeBatch, which must remove all
// of them in this transaction before success. Single-artifact calls select none.
func (t *transaction) disposeExtraction(ctx context.Context, r model.ExtractionDisposalRequest, selectedFacts, selectedMemories []string) error {
	bindings := append([]model.ExtractionRetentionBinding(nil), r.Bindings...)
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Scope < bindings[j].Scope })
	if len(bindings) == 0 || len(bindings) > 256 {
		return model.ErrInvalid
	}
	for i, b := range bindings {
		if !model.ValidID(b.Scope) || (i > 0 && bindings[i-1].Scope == b.Scope) {
			return model.ErrInvalid
		}
		if err := t.lockRetentionScope(ctx, b.Scope); err != nil {
			return err
		}
	}
	q := ledgerdb.New(t.tx)
	// Read the header before creating its guard so an unknown job is a typed error.
	header, err := q.ExtractionDisposalJobHeader(ctx, ledgerdb.ExtractionDisposalJobHeaderParams{TenantID: t.tenant, ID: r.JobID})
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrDenied
	}
	if err != nil {
		return translate(err)
	}
	allowed := make(map[string]bool, len(bindings))
	for _, b := range bindings {
		allowed[b.Scope] = true
	}
	if !allowed[header.ScopeID] {
		return model.ErrDenied
	}
	if err = q.LockExtractionContentGuard(ctx, ledgerdb.LockExtractionContentGuardParams{TenantID: t.tenant, JobID: r.JobID}); err != nil {
		return err
	}
	normalized, err := q.ExtractionDisposalScopes(ctx, ledgerdb.ExtractionDisposalScopesParams{TenantID: t.tenant, ID: r.JobID})
	if err != nil {
		return err
	}
	for _, scope := range normalized {
		if !allowed[scope] {
			return model.ErrDenied
		}
	}
	status, err := t.ExtractionJobStatus(ctx, r.JobID)
	if err != nil {
		return err
	}
	if (status.State != "completed" && status.State != "failed") || status.LeaseUntil != nil {
		return model.ErrPrecondition
	}

	total := 0
	decode := func(payload []byte, dst any) error {
		total += len(payload)
		if len(payload) == 0 || total > 128<<20 {
			return model.ErrPrecondition
		}
		return t.decode(payload, dst)
	}
	raw, err := q.GetExtractionJobPayload(ctx, ledgerdb.GetExtractionJobPayloadParams{TenantID: t.tenant, ID: r.JobID})
	if err != nil {
		return err
	}
	var job model.ExtractionJob
	if err = decode(raw, &job); err != nil {
		return err
	}
	if job.PayloadDisposed || job.ID != r.JobID || job.Tenant != t.tenant || job.Request.Scope != header.ScopeID || job.CreatedRevision != header.LedgerRevision || job.ProfileDigest != header.ProfileDigest || model.Digest(job.Profile) != job.ProfileDigest {
		return model.ErrPrecondition
	}
	scopes := map[string]bool{job.Request.Scope: true}
	addScopes := func(groups ...[]string) {
		for _, group := range groups {
			for _, scope := range group {
				scopes[scope] = true
			}
		}
	}
	addScopes(job.RefreshScopes, job.ContentScopes)
	for _, v := range job.DependencyVersions {
		if v.PayloadDisposed || v.Proposer.Tenant != t.tenant || model.FactDigest(v.Proposal) != v.Digest {
			return model.ErrPrecondition
		}
		scopes[v.Proposal.Key.Scope] = true
	}
	for _, target := range job.Request.MemoryTargets {
		for _, d := range target.Dependencies {
			scopes[d.Scope] = true
		}
	}
	addScopes(normalized)
	rows, err := q.ExtractionDisposalAttempts(ctx, ledgerdb.ExtractionDisposalAttemptsParams{TenantID: t.tenant, JobID: r.JobID})
	if err != nil {
		return err
	}
	if len(rows) > 1000 {
		return model.ErrPrecondition
	}
	type artifact struct {
		kind, id string
		value    any
	}
	artifacts := []artifact{{"job", job.ID, model.ExtractionJobTombstone(job)}}
	latest := map[string]pgtype.Timestamptz{"job": header.RecordedAt}
	requiredRevision := header.LedgerRevision
	recordTime := func(kind string, created pgtype.Timestamptz, revision int64) error {
		if !created.Valid || created.InfinityModifier != pgtype.Finite || revision <= 0 {
			return model.ErrPrecondition
		}
		if !latest[kind].Valid || created.Time.After(latest[kind].Time) {
			latest[kind] = created
		}
		if revision > requiredRevision {
			requiredRevision = revision
		}
		return nil
	}
	if err = recordTime("job", header.RecordedAt, header.LedgerRevision); err != nil {
		return err
	}
	for _, row := range rows {
		raw, err = q.GetExtractionAttemptPayload(ctx, ledgerdb.GetExtractionAttemptPayloadParams{TenantID: t.tenant, ID: row.ID})
		if err != nil {
			return err
		}
		var a model.ExtractionAttempt
		if err = decode(raw, &a); err != nil {
			return err
		}
		if a.PayloadDisposed || a.ID != row.ID || a.Tenant != t.tenant || a.JobID != job.ID || a.Scope != row.ScopeID || a.Scope != job.Request.Scope || a.Fence != row.Fence || a.LedgerRevision != row.LedgerRevision || a.ProfileDigest != job.ProfileDigest {
			return model.ErrPrecondition
		}
		if err = t.validateDisposalInputInventory(ctx, a, row); err != nil {
			return err
		}
		addScopes(a.RefreshScopes, a.ContentScopes)
		if err = recordTime("request", row.RecordedAt, row.LedgerRevision); err != nil {
			return err
		}
		artifacts = append(artifacts, artifact{"request", a.ID, model.ExtractionAttemptTombstone(a)})
		if !row.ResultRevision.Valid {
			continue
		}
		raw, err = q.GetExtractionResultPayload(ctx, ledgerdb.GetExtractionResultPayloadParams{TenantID: t.tenant, AttemptID: a.ID})
		if err != nil {
			return err
		}
		var result model.ExtractionResult
		if err = decode(raw, &result); err != nil {
			return err
		}
		if result.PayloadDisposed || result.AttemptID != a.ID || result.JobID != job.ID || result.Tenant != t.tenant || result.Scope != a.Scope || result.RequestDigest != a.RequestDigest || result.LedgerRevision != row.ResultRevision.Int64 {
			return model.ErrPrecondition
		}
		// A truncated capture's digest identifies the full provider response, not its prefix.
		if result.ResponseComplete && model.BytesDigest(result.ResponseBytes) != result.ResponseDigest {
			return model.ErrPrecondition
		}
		addScopes(result.RefreshScopes, result.ContentScopes)
		if err = recordTime("response", row.ResultTime, row.ResultRevision.Int64); err != nil {
			return err
		}
		artifacts = append(artifacts, artifact{"response", a.ID, model.ExtractionResultTombstone(result)})
	}
	if len(scopes) != len(bindings) {
		return model.ErrDenied
	}
	scopeList := make([]string, 0, len(bindings))
	for _, b := range bindings {
		if !scopes[b.Scope] {
			return model.ErrDenied
		}
		scopeList = append(scopeList, b.Scope)
		holds, err := q.ListActiveLegalHolds(ctx, ledgerdb.ListActiveLegalHoldsParams{TenantID: t.tenant, ScopeID: b.Scope})
		if err != nil {
			return err
		}
		if len(holds) != 0 {
			return model.ErrPrecondition
		}
		for _, selection := range []struct{ kind, id, class string }{{"job", b.JobPolicyID, "extraction_job_payload"}, {"request", b.RequestPolicyID, "extraction_request_payload"}, {"response", b.ResponsePolicyID, "extraction_response_payload"}} {
			policy, err := t.RetentionPolicy(ctx, b.Scope, selection.id)
			if err != nil {
				return err
			}
			if policy.ID != selection.id || policy.Scope != b.Scope || policy.RecordClass != selection.class || policy.Purpose != r.Purpose || policy.Jurisdiction != b.Jurisdiction || policy.MinimumAgeSeconds < 0 || policy.MinimumAgeSeconds > 100*366*86400 {
				return model.ErrPrecondition
			}
			if created := latest[selection.kind]; created.Valid {
				mature, err := q.EvidenceAgeMature(ctx, ledgerdb.EvidenceAgeMatureParams{RegisteredAt: created, MinimumAgeSeconds: policy.MinimumAgeSeconds})
				if err != nil {
					return err
				}
				if !mature {
					return model.ErrPrecondition
				}
			}
		}
	}
	blocked, err := q.ExtractionDisposalBlocked(ctx, ledgerdb.ExtractionDisposalBlockedParams{TenantID: t.tenant, JobID: job.ID, Scopes: scopeList, RequiredRevision: requiredRevision, SelectedFactVersions: selectedFacts, SelectedMemoryVersions: selectedMemories})
	if err != nil {
		return err
	}
	if !blocked.Valid || blocked.Bool {
		return model.ErrPrecondition
	}
	for _, a := range artifacts {
		payload, err := t.encode(a.value)
		if err != nil {
			return err
		}
		var n int64
		switch a.kind {
		case "job":
			err = q.InsertExtractionJobDisposal(ctx, ledgerdb.InsertExtractionJobDisposalParams{TenantID: t.tenant, JobID: a.id, Payload: payload})
			if err == nil {
				n, err = q.DeleteExtractionJobPayload(ctx, ledgerdb.DeleteExtractionJobPayloadParams{TenantID: t.tenant, JobID: a.id})
			}
		case "request":
			err = q.InsertExtractionAttemptDisposal(ctx, ledgerdb.InsertExtractionAttemptDisposalParams{TenantID: t.tenant, AttemptID: a.id, Payload: payload})
			if err == nil {
				n, err = q.DeleteExtractionAttemptPayload(ctx, ledgerdb.DeleteExtractionAttemptPayloadParams{TenantID: t.tenant, AttemptID: a.id})
			}
		case "response":
			err = q.InsertExtractionResultDisposal(ctx, ledgerdb.InsertExtractionResultDisposalParams{TenantID: t.tenant, AttemptID: a.id, Payload: payload})
			if err == nil {
				n, err = q.DeleteExtractionResultPayload(ctx, ledgerdb.DeleteExtractionResultPayloadParams{TenantID: t.tenant, AttemptID: a.id})
			}
		}
		if err != nil {
			return err
		}
		if n != 1 {
			return model.ErrPrecondition
		}
	}
	return nil
}

func (t *transaction) validateDisposalInputInventory(ctx context.Context, a model.ExtractionAttempt, row ledgerdb.ExtractionDisposalAttemptsRow) error {
	facts, memories, err := extractionInputReferences(a)
	if err != nil {
		return model.ErrPrecondition
	}
	if !row.InventoryVersion.Valid || row.InventoryVersion.Int32 != 1 || !row.RequestDigest.Valid || row.RequestDigest.String != a.RequestDigest || !row.FactCount.Valid || int(row.FactCount.Int32) != len(facts) || !row.MemoryCount.Valid || int(row.MemoryCount.Int32) != len(memories) {
		return model.ErrPrecondition
	}
	q := ledgerdb.New(t.tx)
	factRows, err := q.ExtractionDisposalFactInputs(ctx, ledgerdb.ExtractionDisposalFactInputsParams{TenantID: t.tenant, AttemptID: a.ID})
	if err != nil {
		return err
	}
	if len(factRows) != len(facts) {
		return model.ErrPrecondition
	}
	for i, v := range facts {
		if v.RecordID != factRows[i].RecordID || v.VersionID != factRows[i].VersionID || v.PayloadRetained != factRows[i].PayloadRetained {
			return model.ErrPrecondition
		}
	}
	memoryRows, err := q.ExtractionDisposalMemoryInputs(ctx, ledgerdb.ExtractionDisposalMemoryInputsParams{TenantID: t.tenant, AttemptID: a.ID})
	if err != nil {
		return err
	}
	if len(memoryRows) != len(memories) {
		return model.ErrPrecondition
	}
	for i, v := range memories {
		if v.RecordID != memoryRows[i].RecordID || v.VersionID != memoryRows[i].VersionID || v.PayloadRetained != memoryRows[i].PayloadRetained {
			return model.ErrPrecondition
		}
	}
	return nil
}
