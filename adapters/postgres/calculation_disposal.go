package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgtype"
	"slices"
	"sort"
)

func (t *transaction) guardCalculationContent(ctx context.Context, id string) error {
	q := ledgerdb.New(t.tx)
	if err := q.LockCalculationGuard(ctx, ledgerdb.LockCalculationGuardParams{TenantID: t.tenant, CalculationID: id}); err != nil {
		return err
	}
	available, err := q.CalculationContentAvailable(ctx, ledgerdb.CalculationContentAvailableParams{TenantID: t.tenant, CalculationID: id})
	if err != nil {
		return err
	}
	if !available.Valid || !available.Bool {
		return model.ErrPrecondition
	}
	return nil
}

// DisposeCalculation is used by the batch coordinator after normalization and
// fresh authorization. All reference and policy checks remain in this transaction.
func (t *transaction) DisposeCalculation(ctx context.Context, r model.CalculationDisposalRequest) error {
	q := ledgerdb.New(t.tx)
	inventory, err := q.CalculationRetentionScopes(ctx, ledgerdb.CalculationRetentionScopesParams{TenantID: t.tenant, ID: r.CalculationID})
	if err != nil {
		return err
	}
	if len(inventory) == 0 || len(inventory) != len(r.Bindings) {
		return model.ErrDenied
	}
	scopes := map[string]bool{}
	for _, b := range r.Bindings {
		scopes[b.Scope] = true
	}
	for _, scope := range inventory {
		if !scopes[scope] {
			return model.ErrDenied
		}
	}
	for _, b := range r.Bindings {
		if err = t.lockRetentionScope(ctx, b.Scope); err != nil {
			return err
		}
	}
	if err = t.guardCalculationContent(ctx, r.CalculationID); err != nil {
		return err
	}
	record, err := t.Calculation(ctx, r.CalculationID)
	if err != nil {
		return err
	}
	header, err := q.CalculationRetentionHeader(ctx, ledgerdb.CalculationRetentionHeaderParams{TenantID: t.tenant, ID: r.CalculationID})
	if err != nil {
		return err
	}
	receipt := record.Receipt
	if record.PayloadDisposed || record.ID != r.CalculationID || record.Tenant != t.tenant || model.CalculationDigest(receipt) != record.Digest || record.Digest != header.RequestDigest || record.LedgerRevision != header.LedgerRevision || receipt.Scope != header.ScopeID || receipt.SnapshotID != header.SnapshotID || receipt.ApplicationID != header.ApplicationID || receipt.ExternalRunID != header.ExternalRunID {
		return model.ErrPrecondition
	}
	snapshots := append([]string{receipt.SnapshotID}, receipt.ReferenceSnapshotIDs...)
	sort.Strings(snapshots)
	if !slices.Equal(snapshots, header.Snapshots) || !slices.Equal([]string{"input/" + receipt.Input.EvidenceID, "output/" + receipt.Output.EvidenceID}, header.Artifacts) {
		return model.ErrPrecondition
	}
	actual := map[string]bool{receipt.Scope: true}
	for _, id := range snapshots {
		snapshot, err := t.Snapshot(ctx, id)
		if err != nil {
			return err
		}
		if snapshot.Tenant != t.tenant || snapshot.ID != id {
			return model.ErrPrecondition
		}
		if id == receipt.SnapshotID && snapshot.Digest != record.SnapshotDigest {
			return model.ErrPrecondition
		}
		for _, item := range snapshot.Items {
			actual[item.Selector.Scope] = true
		}
	}
	for _, ref := range []model.ArtifactReference{receipt.Input, receipt.Output} {
		evidence, err := t.Evidence(ctx, ref.EvidenceID)
		if err != nil {
			return err
		}
		if evidence.Object.Digest != ref.Digest {
			return model.ErrPrecondition
		}
		actual[evidence.Scope] = true
	}
	if len(actual) != len(scopes) {
		return model.ErrPrecondition
	}
	for scope := range actual {
		if !scopes[scope] {
			return model.ErrDenied
		}
	}
	for _, b := range r.Bindings {
		policy, err := t.RetentionPolicy(ctx, b.Scope, b.PolicyID)
		if err != nil {
			return err
		}
		if policy.RecordClass != "calculation_payload" || policy.Purpose != r.Purpose || policy.Jurisdiction != b.Jurisdiction {
			return model.ErrPrecondition
		}
		holds, err := q.ListActiveLegalHolds(ctx, ledgerdb.ListActiveLegalHoldsParams{TenantID: t.tenant, ScopeID: b.Scope})
		if err != nil {
			return err
		}
		if len(holds) > 0 {
			return model.ErrPrecondition
		}
		mature, err := q.EvidenceAgeMature(ctx, ledgerdb.EvidenceAgeMatureParams{RegisteredAt: header.RecordedAt, MinimumAgeSeconds: policy.MinimumAgeSeconds})
		if err != nil {
			return err
		}
		if !mature {
			return model.ErrPrecondition
		}
	}
	blocked, err := q.CalculationDisposalBlocked(ctx, ledgerdb.CalculationDisposalBlockedParams{TenantID: t.tenant, CalculationID: pgtype.Text{String: r.CalculationID, Valid: true}, Scopes: inventory, RequiredRevision: header.LedgerRevision})
	if err != nil {
		return err
	}
	if !blocked.Valid || blocked.Bool {
		return model.ErrPrecondition
	}
	metadata, err := t.encode(model.CalculationTombstone(record))
	if err != nil {
		return err
	}
	if err = q.InsertCalculationDisposal(ctx, ledgerdb.InsertCalculationDisposalParams{TenantID: t.tenant, CalculationID: r.CalculationID, Payload: metadata}); err != nil {
		return err
	}
	n, err := q.DeleteCalculationPayload(ctx, ledgerdb.DeleteCalculationPayloadParams{TenantID: t.tenant, CalculationID: r.CalculationID})
	if err != nil {
		return err
	}
	if n != 1 {
		return model.ErrPrecondition
	}
	return nil
}
