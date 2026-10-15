-- name: LockCalculationGuard :exec
INSERT INTO calculation_guards(tenant_id,calculation_id,epoch) VALUES($1,$2,1)
ON CONFLICT(tenant_id,calculation_id) DO UPDATE SET epoch=calculation_guards.epoch+1;

-- name: CalculationContentAvailable :one
SELECT EXISTS(SELECT 1 FROM calculation_payloads p WHERE p.tenant_id=$1 AND p.calculation_id=$2)
AND NOT EXISTS(SELECT 1 FROM calculation_disposals d WHERE d.tenant_id=$1 AND d.calculation_id=$2);

-- name: CalculationRetentionScopes :many
SELECT c.scope_id FROM calculation_receipts c WHERE c.tenant_id=$1 AND c.id=$2
UNION SELECT i.scope_id FROM calculation_snapshots s JOIN fact_snapshot_items i ON i.tenant_id=s.tenant_id AND i.snapshot_id=s.snapshot_id WHERE s.tenant_id=$1 AND s.calculation_id=$2
UNION SELECT e.scope_id FROM calculation_artifacts a JOIN evidence e ON e.tenant_id=a.tenant_id AND e.id=a.evidence_id WHERE a.tenant_id=$1 AND a.calculation_id=$2;

-- name: CalculationRetentionHeader :one
SELECT c.scope_id,c.snapshot_id,c.application_id,c.external_run_id,c.request_digest,c.ledger_revision,e.recorded_at,
ARRAY(SELECT s.snapshot_id FROM calculation_snapshots s WHERE s.tenant_id=c.tenant_id AND s.calculation_id=c.id ORDER BY s.snapshot_id)::text[] AS snapshots,
ARRAY(SELECT a.role||'/'||a.evidence_id FROM calculation_artifacts a WHERE a.tenant_id=c.tenant_id AND a.calculation_id=c.id ORDER BY a.role)::text[] AS artifacts
FROM calculation_receipts c JOIN ledger_events e ON e.tenant_id=c.tenant_id AND e.ledger_revision=c.ledger_revision WHERE c.tenant_id=$1 AND c.id=$2;

-- name: CalculationDisposalBlocked :one
SELECT EXISTS(SELECT 1 FROM decision_receipts d WHERE d.tenant_id=sqlc.arg(tenant_id) AND d.calculation_id=sqlc.arg(calculation_id))
OR EXISTS(SELECT 1 FROM audit_export_requests a WHERE a.tenant_id=sqlc.arg(tenant_id) AND a.scope_id=ANY(sqlc.arg(scopes)::text[]))
OR EXISTS(SELECT 1 FROM consumer_offsets c WHERE c.tenant_id=sqlc.arg(tenant_id) AND c.consumer_id LIKE 'archive-%' AND c.acknowledged_revision<sqlc.arg(required_revision)::bigint);

-- name: InsertCalculationDisposal :exec
INSERT INTO calculation_disposals(tenant_id,calculation_id,payload) VALUES($1,$2,$3);

-- name: DeleteCalculationPayload :execrows
DELETE FROM calculation_payloads WHERE tenant_id=$1 AND calculation_id=$2;
