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

func (t *transaction) DisposeVersion(ctx context.Context, r model.VersionDisposalRequest) (int64, error) {
	if r.Kind != "fact" && r.Kind != "memory" {
		return 0, model.ErrInvalid
	}
	bindings := append([]model.VersionRetentionBinding(nil), r.Bindings...)
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Scope < bindings[j].Scope })
	if len(bindings) == 0 || len(bindings) > 256 {
		return 0, model.ErrInvalid
	}
	allowed := map[string]bool{}
	scopes := make([]string, 0, len(bindings))
	for _, b := range bindings {
		if !model.ValidID(b.Scope) || allowed[b.Scope] {
			return 0, model.ErrInvalid
		}
		allowed[b.Scope] = true
		scopes = append(scopes, b.Scope)
		if err := t.lockRetentionScope(ctx, b.Scope); err != nil {
			return 0, err
		}
	}
	q := ledgerdb.New(t.tx)
	var scope, record, digest string
	var created pgtype.Timestamptz
	var createdRevision int64
	if r.Kind == "fact" {
		h, err := q.FactDisposalHeader(ctx, ledgerdb.FactDisposalHeaderParams{TenantID: t.tenant, ID: r.VersionID})
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, model.ErrDenied
		}
		if err != nil {
			return 0, err
		}
		scope, record, digest, created, createdRevision = h.ScopeID, h.RecordID, h.Digest, h.RecordedAt, h.LedgerRevision
	} else {
		h, err := q.MemoryDisposalHeader(ctx, ledgerdb.MemoryDisposalHeaderParams{TenantID: t.tenant, ID: r.VersionID})
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, model.ErrDenied
		}
		if err != nil {
			return 0, err
		}
		scope, record, created, createdRevision = h.ScopeID, h.RecordID, h.RecordedAt, h.LedgerRevision
	}
	if scope != r.Scope || record != r.RecordID || !allowed[scope] {
		return 0, model.ErrDenied
	}
	required := map[string]bool{scope: true}
	if r.Kind == "memory" {
		all, err := q.MemoryDisposalScopes(ctx, ledgerdb.MemoryDisposalScopesParams{TenantID: t.tenant, ID: r.VersionID})
		if err != nil {
			return 0, err
		}
		for _, s := range all {
			required[s] = true
		}
	}
	if len(required) != len(allowed) {
		return 0, model.ErrDenied
	}
	for s := range required {
		if !allowed[s] {
			return 0, model.ErrDenied
		}
	}
	if err := q.LockProjectionRetentionGuard(ctx, t.tenant); err != nil {
		return 0, err
	}
	frozen, err := q.HasFrozenProjection(ctx, t.tenant)
	if err != nil {
		return 0, err
	}
	if frozen {
		return 0, model.ErrPrecondition
	}
	missing, err := q.ListUnmappedProjectionConsumers(ctx, t.tenant)
	if err != nil {
		return 0, err
	}
	if len(missing) > 0 {
		return 0, model.ErrPrecondition
	}
	destinations, err := q.ListProjectionDestinations(ctx, t.tenant)
	if err != nil {
		return 0, err
	}
	if len(destinations) > 10000 {
		return 0, model.ErrPrecondition
	}
	// Increment the record revision at the end, so a lifecycle writer that read
	// content before this transaction cannot commit that stale content afterwards.
	revision, err := q.LockRecord(ctx, ledgerdb.LockRecordParams{TenantID: t.tenant, ID: r.RecordID})
	if err != nil {
		return 0, err
	}
	if err = t.lockVersionContent(ctx, r.Kind == "memory", r.VersionID); err != nil {
		return 0, err
	}
	if !created.Valid || created.InfinityModifier != pgtype.Finite || createdRevision < 1 {
		return 0, model.ErrPrecondition
	}
	var tombstone any
	if r.Kind == "fact" {
		v, err := t.Version(ctx, r.RecordID, r.VersionID)
		if err != nil {
			return 0, err
		}
		if v.ID != r.VersionID || v.RecordID != record || v.Proposer.Tenant != t.tenant || v.Proposal.Key.Scope != scope || v.Digest != digest || model.FactDigest(v.Proposal) != digest {
			return 0, model.ErrPrecondition
		}
		tombstone = model.VersionTombstone(v)
	} else {
		v, err := t.MemoryVersion(ctx, r.RecordID, r.VersionID)
		if err != nil {
			return 0, err
		}
		if v.ID != r.VersionID || v.RecordID != record || v.Proposer.Tenant != t.tenant || v.Proposal.Key.Scope != scope || v.LedgerRevision != createdRevision || model.MemoryDigest(v.Proposal) != v.Digest {
			return 0, model.ErrPrecondition
		}
		actual := map[string]bool{scope: true}
		for _, d := range v.Proposal.Dependencies {
			actual[d.Scope] = true
		}
		if len(actual) != len(required) {
			return 0, model.ErrPrecondition
		}
		for s := range actual {
			if !required[s] {
				return 0, model.ErrPrecondition
			}
		}
		tombstone = model.MemoryVersionTombstone(v)
	}
	for _, b := range bindings {
		policy, err := t.RetentionPolicy(ctx, b.Scope, b.PolicyID)
		if err != nil {
			return 0, err
		}
		if policy.ID != b.PolicyID || policy.Scope != b.Scope || policy.RecordClass != r.Kind+"_payload" || policy.Purpose != r.Purpose || policy.Jurisdiction != b.Jurisdiction || policy.MinimumAgeSeconds < 0 || policy.MinimumAgeSeconds > 100*366*86400 {
			return 0, model.ErrPrecondition
		}
		holds, err := q.ListActiveLegalHolds(ctx, ledgerdb.ListActiveLegalHoldsParams{TenantID: t.tenant, ScopeID: b.Scope})
		if err != nil {
			return 0, err
		}
		if len(holds) > 0 {
			return 0, model.ErrPrecondition
		}
		mature, err := q.EvidenceAgeMature(ctx, ledgerdb.EvidenceAgeMatureParams{RegisteredAt: created, MinimumAgeSeconds: policy.MinimumAgeSeconds})
		if err != nil {
			return 0, err
		}
		if !mature {
			return 0, model.ErrPrecondition
		}
	}
	blocked, err := q.VersionDisposalCommonBlocked(ctx, ledgerdb.VersionDisposalCommonBlockedParams{TenantID: t.tenant, Scopes: scopes})
	if err != nil {
		return 0, err
	}
	if !blocked.Valid || blocked.Bool {
		return 0, model.ErrPrecondition
	}
	if r.Kind == "fact" {
		blocked, err = q.FactDisposalBlocked(ctx, ledgerdb.FactDisposalBlockedParams{TenantID: t.tenant, VersionID: r.VersionID})
	} else {
		blocked, err = q.MemoryDisposalBlocked(ctx, ledgerdb.MemoryDisposalBlockedParams{TenantID: t.tenant, VersionID: r.VersionID})
	}
	if err != nil {
		return 0, err
	}
	if !blocked.Valid || blocked.Bool {
		return 0, model.ErrPrecondition
	}
	payload, err := t.encode(tombstone)
	if err != nil {
		return 0, err
	}
	var removed int64
	if r.Kind == "fact" {
		err = q.InsertFactVersionDisposal(ctx, ledgerdb.InsertFactVersionDisposalParams{TenantID: t.tenant, VersionID: r.VersionID, Payload: payload})
		if err == nil {
			removed, err = q.DeleteFactVersionPayload(ctx, ledgerdb.DeleteFactVersionPayloadParams{TenantID: t.tenant, VersionID: r.VersionID})
		}
	} else {
		err = q.InsertMemoryVersionDisposal(ctx, ledgerdb.InsertMemoryVersionDisposalParams{TenantID: t.tenant, VersionID: r.VersionID, Payload: payload})
		if err == nil {
			removed, err = q.DeleteMemoryVersionPayload(ctx, ledgerdb.DeleteMemoryVersionPayloadParams{TenantID: t.tenant, VersionID: r.VersionID})
		}
	}
	if err != nil {
		return 0, err
	}
	if removed != 1 {
		return 0, model.ErrPrecondition
	}
	if err = t.SetRevision(ctx, record, revision+1); err != nil {
		return 0, err
	}
	return revision + 1, nil
}
