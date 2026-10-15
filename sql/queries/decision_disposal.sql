-- name: LockDecisionGuard :exec
INSERT INTO decision_guards(tenant_id,decision_id,epoch) VALUES($1,$2,1)
ON CONFLICT(tenant_id,decision_id) DO UPDATE SET epoch=decision_guards.epoch+1;

-- name: DecisionContentAvailable :one
SELECT EXISTS(SELECT 1 FROM decision_payloads p WHERE p.tenant_id=$1 AND p.decision_id=$2)
AND NOT EXISTS(SELECT 1 FROM decision_disposals d WHERE d.tenant_id=$1 AND d.decision_id=$2);

-- name: DecisionRetentionScopes :many
SELECT s.scope_id FROM decision_scopes s WHERE s.tenant_id=$1 AND s.decision_id=$2 ORDER BY s.scope_id COLLATE "C";

-- name: DecisionRetentionHeader :one
SELECT d.scope_id,d.application_id,d.external_decision_id,d.request_digest,d.context_id,d.snapshot_id,d.calculation_id,d.ledger_revision,e.recorded_at,
ARRAY(SELECT a.role||'/'||a.evidence_id FROM decision_artifacts a WHERE a.tenant_id=d.tenant_id AND a.decision_id=d.id ORDER BY a.role COLLATE "C")::text[] AS artifacts,
ARRAY(SELECT value FROM (
 SELECT 'fact/'||i.role||'/'||i.ordinal::text||'/'||i.record_id||'/'||i.version_id||'/'||i.scope_id AS value FROM decision_items i WHERE i.tenant_id=d.tenant_id AND i.decision_id=d.id
 UNION ALL SELECT 'memory/'||i.role||'/'||i.ordinal::text||'/'||i.record_id||'/'||i.version_id||'/'||i.scope_id FROM decision_memory_items i WHERE i.tenant_id=d.tenant_id AND i.decision_id=d.id
) items ORDER BY value COLLATE "C")::text[] AS items
FROM decision_receipts d JOIN ledger_events e ON e.tenant_id=d.tenant_id AND e.ledger_revision=d.ledger_revision WHERE d.tenant_id=$1 AND d.id=$2;

-- name: DecisionDisposalBlocked :one
SELECT EXISTS(SELECT 1 FROM audit_export_requests a WHERE a.tenant_id=sqlc.arg(tenant_id) AND a.scope_id=ANY(sqlc.arg(scopes)::text[]))
OR EXISTS(SELECT 1 FROM consumer_offsets c WHERE c.tenant_id=sqlc.arg(tenant_id) AND c.consumer_id LIKE 'archive-%' AND c.acknowledged_revision<sqlc.arg(required_revision)::bigint);

-- name: InsertDecisionDisposal :exec
INSERT INTO decision_disposals(tenant_id,decision_id,payload) VALUES($1,$2,$3);

-- name: DeleteDecisionPayload :execrows
DELETE FROM decision_payloads WHERE tenant_id=$1 AND decision_id=$2;
