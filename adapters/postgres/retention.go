package postgres

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
)

func (l *Ledger) RetentionDomain(ctx context.Context) (string, error) {
	var id string
	err := l.pool.QueryRow(ctx, `SELECT 'postgres:'||identity::text FROM retention_domain WHERE id`).Scan(&id)
	return id, err
}
func (t *transaction) lockRetentionScope(ctx context.Context, scope string) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO retention_scopes VALUES($1,$2,1) ON CONFLICT(tenant_id,scope_id) DO UPDATE SET epoch=retention_scopes.epoch+1`, t.tenant, scope)
	return err
}
func (t *transaction) lockEvidenceObject(ctx context.Context, ref model.ObjectRef) error {
	var disposed bool
	err := t.tx.QueryRow(ctx, `INSERT INTO retention_objects(tenant_id,locator,object_version,epoch) VALUES($1,$2,$3,1) ON CONFLICT(tenant_id,locator,object_version) DO UPDATE SET epoch=retention_objects.epoch+1 RETURNING disposed`, t.tenant, ref.Locator, ref.Version).Scan(&disposed)
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
	var disposed bool
	err = t.tx.QueryRow(ctx, `SELECT disposed FROM retention_objects WHERE tenant_id=$1 AND locator=$2 AND object_version=$3`, t.tenant, e.Object.Locator, e.Object.Version).Scan(&disposed)
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
	err := t.read(ctx, &p, `SELECT payload FROM retention_policies WHERE tenant_id=$1 AND scope_id=$2 AND id=$3`, t.tenant, scope, id)
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
	err = t.tx.QueryRow(ctx, `SELECT e.registered_at,o.disposed FROM evidence_retention e JOIN retention_objects o USING(tenant_id,locator,object_version) WHERE e.tenant_id=$1 AND e.evidence_id=$2`, t.tenant, id).Scan(&out.RegisteredAt, &out.Disposed)
	if errors.Is(err, pgx.ErrNoRows) {
		out.Blockers = append(out.Blockers, "legacy_binding_unavailable")
	} else if err != nil {
		return out, err
	}
	rows, err := t.tx.Query(ctx, `SELECT h.id FROM legal_holds h WHERE h.tenant_id=$1 AND h.scope_id=$2 AND NOT EXISTS(SELECT 1 FROM legal_hold_releases r WHERE r.tenant_id=h.tenant_id AND r.scope_id=h.scope_id AND r.id=h.id) ORDER BY h.id`, t.tenant, scope)
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
		{"evidence_derivatives", `SELECT EXISTS(SELECT 1 FROM evidence_derivations WHERE tenant_id=$1 AND parent_evidence_id=$2)`},
		{"conversation_messages", `SELECT EXISTS(SELECT 1 FROM conversation_messages WHERE tenant_id=$1 AND evidence_id=$2)`},
		{"memory_versions", `SELECT EXISTS(SELECT 1 FROM memory_evidence WHERE tenant_id=$1 AND evidence_id=$2)`},
		{"fact_versions", `SELECT EXISTS(SELECT 1 FROM version_evidence WHERE tenant_id=$1 AND evidence_id=$2)`},
		{"confirmation_artifacts", `SELECT EXISTS(SELECT 1 FROM confirmation_artifacts WHERE tenant_id=$1 AND evidence_id=$2)`},
		{"memory_attestations", `SELECT EXISTS(SELECT 1 FROM memory_attestations WHERE tenant_id=$1 AND evidence_id=$2)`},
		{"attestations", `SELECT EXISTS(SELECT 1 FROM attestations WHERE tenant_id=$1 AND evidence_id=$2)`},
		{"calculation_artifacts", `SELECT EXISTS(SELECT 1 FROM calculation_artifacts WHERE tenant_id=$1 AND evidence_id=$2)`},
		{"decision_artifacts", `SELECT EXISTS(SELECT 1 FROM decision_artifacts WHERE tenant_id=$1 AND evidence_id=$2)`},
		{"export_copy_or_pending_export", `SELECT EXISTS(SELECT 1 FROM audit_export_requests a JOIN evidence e ON e.tenant_id=a.tenant_id AND e.scope_id=a.scope_id WHERE e.tenant_id=$1 AND e.id=$2)`},
		{"shared_object", `SELECT EXISTS(SELECT 1 FROM evidence_retention a JOIN evidence_retention b USING(tenant_id,locator,object_version) WHERE a.tenant_id=$1 AND a.evidence_id=$2 AND b.evidence_id<>a.evidence_id)`},
		// Legacy encrypted metadata may alias this object; never guess it is independent.
		{"legacy_inventory_incomplete", `SELECT EXISTS(SELECT 1 FROM evidence e WHERE e.tenant_id=$1 AND e.id<>$2 AND NOT EXISTS(SELECT 1 FROM evidence_retention b WHERE b.tenant_id=e.tenant_id AND b.evidence_id=e.id))`},
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
	if err := t.lockRetentionScope(ctx, r.Scope); err != nil {
		return err
	}
	e, err := t.Evidence(ctx, r.EvidenceID)
	if err != nil {
		return err
	}
	if e.Scope != r.Scope {
		return model.ErrDenied
	}
	if err = t.lockEvidenceObject(ctx, e.Object); err != nil {
		return err
	}
	status, err := t.RetentionStatus(ctx, r.Scope, r.EvidenceID)
	if err != nil {
		return err
	}
	if len(status.ActiveHolds) > 0 || len(status.Blockers) > 0 || status.Disposed || status.PolicyID != r.PolicyID {
		return model.ErrPrecondition
	}
	policy, err := t.RetentionPolicy(ctx, r.Scope, r.PolicyID)
	if err != nil {
		return err
	}
	if policy.RecordClass != "evidence_bytes" || policy.Purpose != r.Purpose || policy.Jurisdiction != r.Jurisdiction {
		return model.ErrPrecondition
	}
	var mature bool
	if err = t.tx.QueryRow(ctx, `SELECT clock_timestamp()>=$1::timestamptz+($2::bigint*interval '1 second')`, status.RegisteredAt, policy.MinimumAgeSeconds).Scan(&mature); err != nil {
		return err
	}
	if !mature {
		return model.ErrPrecondition
	}
	deleted, err := t.tx.Exec(ctx, `DELETE FROM evidence_objects WHERE tenant_id=$1 AND locator=$2 AND object_version=$3`, t.tenant, e.Object.Locator, e.Object.Version)
	if err != nil {
		return err
	}
	if deleted.RowsAffected() != 1 {
		return model.ErrPrecondition
	}
	_, err = t.tx.Exec(ctx, `UPDATE retention_objects SET disposed=true WHERE tenant_id=$1 AND locator=$2 AND object_version=$3`, t.tenant, e.Object.Locator, e.Object.Version)
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
			_, err = t.tx.Exec(ctx, `INSERT INTO retention_policies VALUES($1,$2,$3,$4)`, t.tenant, r.Scope, r.TargetID, b)
		}
	case "place_legal_hold":
		_, err = t.tx.Exec(ctx, `INSERT INTO legal_holds VALUES($1,$2,$3)`, t.tenant, r.Scope, r.TargetID)
	case "release_legal_hold":
		_, err = t.tx.Exec(ctx, `INSERT INTO legal_hold_releases VALUES($1,$2,$3)`, t.tenant, r.Scope, r.TargetID)
	}
	if err != nil {
		return err
	}
	b, err := t.encode(r)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO retention_events VALUES($1,$2,$3,$4,$5,$6,$7)`, t.tenant, r.ID, r.Scope, r.Kind, r.TargetID, event.LedgerRevision, b)
	return err
}

var _ model.RetentionTx = (*transaction)(nil)
