package postgres

import (
	"context"
	"errors"
	"sort"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
)

func (l *Ledger) RetentionDomain(ctx context.Context) (string, error) {
	id, err := ledgerdb.New(l.pool).GetRetentionDomain(ctx)
	return id, err
}
func (t *transaction) lockRetentionScope(ctx context.Context, scope string) error {
	_, err := ledgerdb.New(t.tx).LockRetentionScope(ctx, ledgerdb.LockRetentionScopeParams{TenantID: t.tenant, ScopeID: scope})
	return err
}
func (t *transaction) lockEvidenceObject(ctx context.Context, ref model.ObjectRef) error {
	disposed, err := ledgerdb.New(t.tx).LockRetentionObject(ctx, ledgerdb.LockRetentionObjectParams{TenantID: t.tenant, Locator: ref.Locator, ObjectVersion: ref.Version})
	if err != nil {
		return err
	}
	if disposed {
		return model.ErrPrecondition
	}
	return nil
}
func (t *transaction) guardEvidenceReference(ctx context.Context, id string) error {
	e, err := t.Evidence(ctx, id)
	if err != nil {
		return err
	}
	if err = t.lockRetentionScope(ctx, e.Scope); err != nil {
		return err
	}
	disposed, err := ledgerdb.New(t.tx).GetObjectDisposed(ctx, ledgerdb.GetObjectDisposedParams{TenantID: t.tenant, Locator: e.Object.Locator, ObjectVersion: e.Object.Version})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if disposed {
		return model.ErrPrecondition
	}
	return nil
}
func (t *transaction) RetentionPolicy(ctx context.Context, scope, id string) (model.RetentionPolicy, error) {
	var p model.RetentionPolicy
	payload, err := ledgerdb.New(t.tx).GetRetentionPolicyPayload(ctx, ledgerdb.GetRetentionPolicyPayloadParams{TenantID: t.tenant, ScopeID: scope, ID: id})
	if err != nil {
		return p, translate(err)
	}
	err = t.decode(payload, &p)
	return p, err
}
func (t *transaction) StageRetention(ctx context.Context, e model.RetentionEvent) error {
	if err := t.lockRetentionScope(ctx, e.Scope); err != nil {
		return err
	}
	t.pendingRetention = &e
	return nil
}
func (t *transaction) RetentionStatus(ctx context.Context, scope, id string) (model.RetentionStatus, error) {
	out := model.RetentionStatus{EvidenceID: id, ActiveHolds: []string{}, Blockers: []string{}}
	e, err := t.Evidence(ctx, id)
	if err != nil {
		return out, err
	}
	if e.Scope != scope {
		return out, model.ErrDenied
	}
	out.PolicyID = e.RetentionPolicy
	err = t.tx.QueryRow(ctx, ledgerdb.GetEvidenceRetentionStatus, t.tenant, id).Scan(&out.RegisteredAt, &out.Disposed)
	if errors.Is(err, pgx.ErrNoRows) {
		out.Blockers = append(out.Blockers, "legacy_binding_unavailable")
	} else if err != nil {
		return out, err
	}
	rows, err := t.tx.Query(ctx, ledgerdb.ListActiveLegalHolds, t.tenant, scope)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return out, err
		}
		out.ActiveHolds = append(out.ActiveHolds, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	checks := []struct{ reason, query string }{
		{"extraction_redactions", ledgerdb.HasExtractionRedactionReference},
		{"evidence_derivatives", ledgerdb.HasEvidenceDerivative},
		{"conversation_messages", ledgerdb.HasConversationReference},
		{"memory_versions", ledgerdb.HasMemoryEvidenceReference},
		{"fact_versions", ledgerdb.HasFactEvidenceReference},
		{"confirmation_artifacts", ledgerdb.HasConfirmationReference},
		{"memory_attestations", ledgerdb.HasMemoryAttestationReference},
		{"attestations", ledgerdb.HasFactAttestationReference},
		{"calculation_artifacts", ledgerdb.HasCalculationArtifactReference},
		{"decision_artifacts", ledgerdb.HasDecisionArtifactReference},
		{"export_copy_or_pending_export", ledgerdb.HasAuditExportReference},
		{"shared_object", ledgerdb.HasSharedEvidenceObject},
		// Legacy encrypted metadata may alias this object; never guess it is independent.
		{"legacy_inventory_incomplete", ledgerdb.HasUnboundLegacyEvidence},
	}
	for _, check := range checks {
		var blocked bool
		if err = t.tx.QueryRow(ctx, check.query, t.tenant, id).Scan(&blocked); err != nil {
			return out, err
		}
		if blocked {
			out.Blockers = append(out.Blockers, check.reason)
		}
	}
	return out, nil
}
func (t *transaction) DisposeEvidence(ctx context.Context, r model.EvidenceDisposalRequest) error {
	if len(r.DerivativeEvidenceIDs) != 0 {
		return model.ErrPrecondition
	}
	e, _, err := t.prepareEvidenceDisposal(ctx, r, false, false, nil)
	if err != nil {
		return err
	}
	return t.disposeEvidenceObject(ctx, e)
}

func (t *transaction) DisposeEvidenceLineage(ctx context.Context, r model.EvidenceDisposalRequest) error {
	if len(r.DerivativeEvidenceIDs) == 0 || len(r.DerivativeEvidenceIDs) > 256 {
		return model.ErrInvalid
	}
	if err := t.lockRetentionScope(ctx, r.Scope); err != nil {
		return err
	}
	ids := append([]string{r.EvidenceID}, r.DerivativeEvidenceIDs...)
	seen := map[string]bool{}
	for _, id := range ids {
		if !model.ValidID(id) || seen[id] {
			return model.ErrInvalid
		}
		seen[id] = true
	}
	sort.Strings(ids)
	objects := make([]model.Evidence, 0, len(ids))
	seenObjects := map[struct{ locator, version string }]bool{}
	for _, id := range ids {
		item := r
		item.EvidenceID = id
		item.DerivativeEvidenceIDs = nil
		e, disposed, err := t.prepareEvidenceDisposal(ctx, item, id == r.EvidenceID, id != r.EvidenceID, ids)
		if err != nil {
			return err
		}
		if id == r.EvidenceID {
			if e.Derivation != nil {
				return model.ErrPrecondition
			}
		} else if e.Derivation == nil || e.Derivation.ParentEvidenceID != r.EvidenceID {
			return model.ErrPrecondition
		}
		objectKey := struct{ locator, version string }{e.Object.Locator, e.Object.Version}
		if !disposed && !seenObjects[objectKey] {
			objects = append(objects, e)
			seenObjects[objectKey] = true
		}
	}
	// The scope lock serializes registration and holds. Repeatable-read conflicts
	// force a retry if a derivative was published after this snapshot began.
	missing, err := ledgerdb.New(t.tx).HasUnselectedEvidenceDerivative(ctx, ledgerdb.HasUnselectedEvidenceDerivativeParams{TenantID: t.tenant, ParentEvidenceID: r.EvidenceID, SelectedIds: r.DerivativeEvidenceIDs})
	if err != nil {
		return err
	}
	if missing {
		return model.ErrPrecondition
	}
	// Every member must pass before any deletion. Immutable metadata and the
	// explicit command membership remain available in the retention event.
	for _, e := range objects {
		if err := t.disposeEvidenceObject(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) prepareEvidenceDisposal(ctx context.Context, r model.EvidenceDisposalRequest, selectedDerivatives, allowDisposed bool, selectedIDs []string) (model.Evidence, bool, error) {
	if err := t.lockRetentionScope(ctx, r.Scope); err != nil {
		return model.Evidence{}, false, err
	}
	e, err := t.Evidence(ctx, r.EvidenceID)
	if err != nil {
		return e, false, err
	}
	if e.Scope != r.Scope {
		return e, false, model.ErrDenied
	}
	disposed, err := ledgerdb.New(t.tx).LockRetentionObject(ctx, ledgerdb.LockRetentionObjectParams{TenantID: t.tenant, Locator: e.Object.Locator, ObjectVersion: e.Object.Version})
	if err != nil {
		return e, false, err
	}
	if disposed && !allowDisposed {
		return e, false, model.ErrPrecondition
	}
	status, err := t.RetentionStatus(ctx, r.Scope, r.EvidenceID)
	if err != nil {
		return e, false, err
	}
	for _, blocker := range status.Blockers {
		if blocker == "evidence_derivatives" && selectedDerivatives {
			continue
		}
		if blocker == "shared_object" && len(selectedIDs) > 0 {
			// The object epoch lock also serializes aliases registered in other scopes.
			// Never waive the blocker while an unselected evidence identity shares it.
			unselected, err := ledgerdb.New(t.tx).HasUnselectedSharedEvidenceObject(ctx, ledgerdb.HasUnselectedSharedEvidenceObjectParams{TenantID: t.tenant, EvidenceID: r.EvidenceID, SelectedIds: selectedIDs})
			if err != nil {
				return e, false, err
			}
			if !unselected {
				continue
			}
		}
		return e, false, model.ErrPrecondition
	}
	if len(status.ActiveHolds) > 0 || (status.Disposed && !allowDisposed) || status.PolicyID != r.PolicyID {
		return e, false, model.ErrPrecondition
	}
	policy, err := t.RetentionPolicy(ctx, r.Scope, r.PolicyID)
	if err != nil {
		return e, false, err
	}
	if policy.RecordClass != "evidence_bytes" || policy.Purpose != r.Purpose || policy.Jurisdiction != r.Jurisdiction {
		return e, false, model.ErrPrecondition
	}
	mature, err := ledgerdb.New(t.tx).EvidenceAgeMature(ctx, ledgerdb.EvidenceAgeMatureParams{RegisteredAt: nullableTime(&status.RegisteredAt), MinimumAgeSeconds: policy.MinimumAgeSeconds})
	if err != nil {
		return e, false, err
	}
	if !mature {
		return e, false, model.ErrPrecondition
	}
	return e, disposed, nil
}

func (t *transaction) disposeEvidenceObject(ctx context.Context, e model.Evidence) error {
	deleted, err := ledgerdb.New(t.tx).DeleteEvidenceObject(ctx, ledgerdb.DeleteEvidenceObjectParams{TenantID: t.tenant, Locator: e.Object.Locator, ObjectVersion: e.Object.Version})
	if err != nil {
		return err
	}
	if deleted != 1 {
		return model.ErrPrecondition
	}
	_, err = ledgerdb.New(t.tx).MarkEvidenceObjectDisposed(ctx, ledgerdb.MarkEvidenceObjectDisposedParams{TenantID: t.tenant, Locator: e.Object.Locator, ObjectVersion: e.Object.Version})
	return err
}
func (t *transaction) flushRetention(ctx context.Context, event model.Event) error {
	if t.pendingRetention == nil {
		return nil
	}
	r := *t.pendingRetention
	r.LedgerRevision = event.LedgerRevision
	var err error
	switch r.Kind {
	case "register_retention_policy":
		var b []byte
		b, err = t.encode(r.Policy)
		if err == nil {
			_, err = ledgerdb.New(t.tx).InsertRetentionPolicyPayload(ctx, ledgerdb.InsertRetentionPolicyPayloadParams{TenantID: t.tenant, ScopeID: r.Scope, ID: r.TargetID, Payload: b})
		}
	case "place_legal_hold":
		_, err = ledgerdb.New(t.tx).InsertLegalHold(ctx, ledgerdb.InsertLegalHoldParams{TenantID: t.tenant, ScopeID: r.Scope, ID: r.TargetID})
	case "release_legal_hold":
		_, err = ledgerdb.New(t.tx).InsertLegalHoldRelease(ctx, ledgerdb.InsertLegalHoldReleaseParams{TenantID: t.tenant, ScopeID: r.Scope, ID: r.TargetID})
	}
	if err != nil {
		return err
	}
	b, err := t.encode(r)
	if err != nil {
		return err
	}
	_, err = ledgerdb.New(t.tx).InsertRetentionEvent(ctx, ledgerdb.InsertRetentionEventParams{TenantID: t.tenant, ID: r.ID, ScopeID: r.Scope, Kind: r.Kind, TargetID: r.TargetID, LedgerRevision: event.LedgerRevision, Payload: b})
	if err == nil && r.ProjectionCleanup != nil {
		cleanup := r.ProjectionCleanup
		err = ledgerdb.New(t.tx).InsertProjectionCleanupAcknowledgement(ctx, ledgerdb.InsertProjectionCleanupAcknowledgementParams{TenantID: t.tenant, Kind: cleanup.Kind, VersionID: cleanup.VersionID, Generation: cleanup.Generation, AcknowledgementID: r.ID})
	}
	return err
}

var _ model.RetentionTx = (*transaction)(nil)
