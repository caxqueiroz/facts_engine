package postgres

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) DisposeDecision(ctx context.Context, r model.DecisionDisposalRequest) error {
	q := ledgerdb.New(t.tx)
	inventory, err := q.DecisionRetentionScopes(ctx, ledgerdb.DecisionRetentionScopesParams{TenantID: t.tenant, DecisionID: r.DecisionID})
	if err != nil {
		return err
	}
	if len(inventory) == 0 || len(inventory) != len(r.Bindings) {
		return model.ErrDenied
	}
	for i, b := range r.Bindings {
		if inventory[i] != b.Scope {
			return model.ErrDenied
		}
		if err = t.lockRetentionScope(ctx, b.Scope); err != nil {
			return err
		}
	}
	if err = q.LockDecisionGuard(ctx, ledgerdb.LockDecisionGuardParams{TenantID: t.tenant, DecisionID: r.DecisionID}); err != nil {
		return err
	}
	available, err := q.DecisionContentAvailable(ctx, ledgerdb.DecisionContentAvailableParams{TenantID: t.tenant, DecisionID: r.DecisionID})
	if err != nil {
		return err
	}
	if !available.Valid || !available.Bool {
		return model.ErrPrecondition
	}
	record, err := t.Decision(ctx, r.DecisionID)
	if err != nil {
		return err
	}
	header, err := q.DecisionRetentionHeader(ctx, ledgerdb.DecisionRetentionHeaderParams{TenantID: t.tenant, ID: r.DecisionID})
	if err != nil {
		return err
	}
	receipt := record.Receipt
	if record.PayloadDisposed || record.ID != r.DecisionID || record.Tenant != t.tenant || model.DecisionDigest(receipt) != record.Digest || record.Digest != header.RequestDigest || record.LedgerRevision != header.LedgerRevision || receipt.Scope != header.ScopeID || receipt.ApplicationID != header.ApplicationID || receipt.ExternalDecisionID != header.ExternalDecisionID || nullableID(receipt.ContextID) != header.ContextID || nullableID(receipt.SnapshotID) != header.SnapshotID || nullableID(receipt.CalculationID) != header.CalculationID || !slices.Equal(record.Scopes, inventory) {
		return model.ErrPrecondition
	}
	if err = t.validateDecisionDisposalInventory(ctx, record, header); err != nil {
		return err
	}
	for _, b := range r.Bindings {
		policy, err := t.RetentionPolicy(ctx, b.Scope, b.PolicyID)
		if err != nil {
			return err
		}
		if policy.RecordClass != "decision_payload" || policy.Purpose != r.Purpose || policy.Jurisdiction != b.Jurisdiction {
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
	blocked, err := q.DecisionDisposalBlocked(ctx, ledgerdb.DecisionDisposalBlockedParams{TenantID: t.tenant, Scopes: inventory, RequiredRevision: header.LedgerRevision})
	if err != nil {
		return err
	}
	if !blocked.Valid || blocked.Bool {
		return model.ErrPrecondition
	}
	metadata, err := t.encode(model.DecisionTombstone(record))
	if err != nil {
		return err
	}
	if err = q.InsertDecisionDisposal(ctx, ledgerdb.InsertDecisionDisposalParams{TenantID: t.tenant, DecisionID: r.DecisionID, Payload: metadata}); err != nil {
		return err
	}
	n, err := q.DeleteDecisionPayload(ctx, ledgerdb.DeleteDecisionPayloadParams{TenantID: t.tenant, DecisionID: r.DecisionID})
	if err != nil {
		return err
	}
	if n != 1 {
		return model.ErrPrecondition
	}
	return nil
}

func (t *transaction) validateDecisionDisposalInventory(ctx context.Context, record model.DecisionRecord, header ledgerdb.DecisionRetentionHeaderRow) error {
	r := record.Receipt
	scopes := map[string]bool{}
	items := map[model.VersionReference]string{}
	addSnapshot := func(id, digest string, source bool) error {
		snapshot, err := t.Snapshot(ctx, id)
		if err != nil {
			return err
		}
		if snapshot.PayloadDisposed || snapshot.ID != id || snapshot.Tenant != t.tenant || model.SnapshotDigest(snapshot) != snapshot.Digest || (digest != "" && snapshot.Digest != digest) {
			return model.ErrPrecondition
		}
		for _, item := range snapshot.Items {
			scopes[item.Selector.Scope] = true
			if source && (item.Resolution.State == "resolved" || item.Resolution.State == "unknown") {
				items[model.VersionReference{RecordID: item.Resolution.RecordID, VersionID: item.Resolution.VersionID}] = "fact/" + item.Selector.Scope
			}
		}
		return nil
	}
	if (r.ContextID == "") == (r.SnapshotID == "") {
		return model.ErrPrecondition
	}
	if r.ContextID != "" {
		m, err := t.ContextManifest(ctx, r.ContextID)
		if err != nil {
			return err
		}
		if m.PayloadDisposed || m.ID != r.ContextID || m.Tenant != t.tenant || m.Digest != record.SourceDigest || model.ContextDigest(m) != m.Digest || model.BytesDigest(m.Rendered) != m.RenderedDigest {
			return model.ErrPrecondition
		}
		for _, scope := range model.ContextScopes(m) {
			scopes[scope] = true
		}
		for _, item := range m.Items {
			items[model.VersionReference{RecordID: item.Fact.RecordID, VersionID: item.Fact.VersionID}] = "fact/" + item.Key.Scope
		}
		for _, item := range m.Memories {
			items[model.VersionReference{RecordID: item.Memory.RecordID, VersionID: item.Memory.VersionID}] = "memory/" + item.Key.Scope
		}
	} else if err := addSnapshot(r.SnapshotID, record.SourceDigest, true); err != nil {
		return err
	}
	addEvidence := func(ref model.ArtifactReference) error {
		evidence, err := t.Evidence(ctx, ref.EvidenceID)
		if err != nil {
			return err
		}
		if evidence.Object.Digest != ref.Digest {
			return model.ErrPrecondition
		}
		scopes[evidence.Scope] = true
		return nil
	}
	if r.CalculationID != "" {
		calc, err := t.Calculation(ctx, r.CalculationID)
		if err != nil {
			return err
		}
		if calc.ID != r.CalculationID || calc.Tenant != t.tenant || calc.Digest != record.CalculationDigest || model.CalculationDigest(calc.Receipt) != calc.Digest || r.SnapshotID == "" || calc.Receipt.SnapshotID != r.SnapshotID {
			return model.ErrPrecondition
		}
		for _, id := range append([]string{calc.Receipt.SnapshotID}, calc.Receipt.ReferenceSnapshotIDs...) {
			if err = addSnapshot(id, "", false); err != nil {
				return err
			}
		}
		for _, ref := range []model.ArtifactReference{calc.Receipt.Input, calc.Receipt.Output} {
			if err = addEvidence(ref); err != nil {
				return err
			}
		}
	} else if record.CalculationDigest != "" {
		return model.ErrPrecondition
	}
	artifacts := []string{}
	for role, ref := range model.DecisionArtifacts(r) {
		if err := addEvidence(ref); err != nil {
			return err
		}
		artifacts = append(artifacts, role+"/"+ref.EvidenceID)
	}
	sort.Strings(artifacts)
	if !slices.Equal(artifacts, header.Artifacts) || !scopes[r.Scope] || len(scopes) != len(record.Scopes) {
		return model.ErrPrecondition
	}
	for _, scope := range record.Scopes {
		if !scopes[scope] {
			return model.ErrPrecondition
		}
	}
	expected := []string{}
	selected := map[model.VersionReference]bool{}
	for _, group := range []struct {
		role string
		refs []model.VersionReference
	}{{"selected", r.SelectedItems}, {"used", r.UsedItems}} {
		seen := map[model.VersionReference]bool{}
		for ordinal, ref := range group.refs {
			kindScope, ok := items[ref]
			if !ok || seen[ref] || (group.role == "used" && !selected[ref]) {
				return model.ErrPrecondition
			}
			seen[ref] = true
			if group.role == "selected" {
				selected[ref] = true
			}
			kind, scope := "fact", kindScope[len("fact/"):]
			if len(kindScope) >= len("memory/") && kindScope[:len("memory/")] == "memory/" {
				kind = "memory"
				scope = kindScope[len("memory/"):]
			}
			expected = append(expected, fmt.Sprintf("%s/%s/%d/%s/%s/%s", kind, group.role, ordinal, ref.RecordID, ref.VersionID, scope))
		}
	}
	sort.Strings(expected)
	if !slices.Equal(expected, header.Items) {
		return model.ErrPrecondition
	}
	return nil
}
