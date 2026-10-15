-- name: FactDisposalHeader :one
SELECT v.scope_id,v.record_id,v.digest,e.ledger_revision,e.recorded_at
FROM record_versions v JOIN LATERAL (
 SELECT e.ledger_revision,e.recorded_at FROM record_events r JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id
 WHERE r.tenant_id=v.tenant_id AND r.version_id=v.id ORDER BY e.ledger_revision LIMIT 1
) e ON true WHERE v.tenant_id=$1 AND v.id=$2;

-- name: MemoryDisposalHeader :one
SELECT v.scope_id,v.record_id,e.ledger_revision,e.recorded_at
FROM memory_versions v JOIN ledger_events e ON e.tenant_id=v.tenant_id AND e.ledger_revision=v.ledger_revision
WHERE v.tenant_id=$1 AND v.id=$2;

-- name: MemoryDisposalScopes :many
SELECT v.scope_id FROM memory_versions v WHERE v.tenant_id=$1 AND v.id=$2
UNION SELECT d.scope_id FROM memory_dependencies d WHERE d.tenant_id=$1 AND d.memory_version_id=$2;

-- name: HasFrozenProjection :one
SELECT EXISTS(SELECT 1 FROM projection_generations WHERE tenant_id=$1 AND state IN ('paused','verified','activating'));

-- name: VersionDisposalCommonBlocked :one
SELECT EXISTS(SELECT 1 FROM audit_export_requests a WHERE a.tenant_id=sqlc.arg(tenant_id) AND a.scope_id=ANY(sqlc.arg(scopes)::text[]))
OR EXISTS(SELECT 1 FROM extraction_attempts a
WHERE a.tenant_id=sqlc.arg(tenant_id) AND NOT EXISTS(SELECT 1 FROM extraction_attempt_inventories i WHERE i.tenant_id=a.tenant_id AND i.attempt_id=a.id));

-- name: FactDisposalBlocked :one
SELECT EXISTS(SELECT 1 FROM fact_snapshot_items i JOIN fact_snapshot_payloads p ON p.tenant_id=i.tenant_id AND p.snapshot_id=i.snapshot_id WHERE i.tenant_id=$1 AND i.version_id=$2)
OR EXISTS(SELECT 1 FROM context_items i JOIN context_payloads p ON p.tenant_id=i.tenant_id AND p.context_id=i.context_id WHERE i.tenant_id=$1 AND i.version_id=$2)
OR EXISTS(SELECT 1 FROM decision_items x WHERE x.tenant_id=$1 AND x.version_id=$2)
OR EXISTS(SELECT 1 FROM attestations x WHERE x.tenant_id=$1 AND x.version_id=$2)
OR EXISTS(SELECT 1 FROM memory_dependencies d JOIN memory_version_payloads p ON p.tenant_id=d.tenant_id AND p.version_id=d.memory_version_id WHERE d.tenant_id=$1 AND d.version_id=$2)
OR EXISTS(SELECT 1 FROM extraction_memory_dependencies d JOIN extraction_job_payloads p ON p.tenant_id=d.tenant_id AND p.job_id=d.job_id WHERE d.tenant_id=$1 AND d.version_id=$2)
OR EXISTS(SELECT 1 FROM extraction_attempt_fact_inputs i JOIN extraction_attempt_payloads p ON p.tenant_id=i.tenant_id AND p.attempt_id=i.attempt_id WHERE i.tenant_id=$1 AND i.version_id=$2 AND i.payload_retained)
OR EXISTS(SELECT 1 FROM extraction_proposals d JOIN extraction_result_payloads p ON p.tenant_id=d.tenant_id AND p.attempt_id=d.attempt_id WHERE d.tenant_id=$1 AND d.version_id=$2)
OR EXISTS(SELECT 1 FROM consumer_offsets c WHERE c.tenant_id=$1 AND c.consumer_id LIKE 'archive-%' AND c.acknowledged_revision < (
 SELECT max(e.ledger_revision) FROM record_events r JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id WHERE r.tenant_id=$1 AND r.version_id=$2));

-- name: MemoryDisposalBlocked :one
SELECT EXISTS(SELECT 1 FROM context_memory_items i JOIN context_payloads p ON p.tenant_id=i.tenant_id AND p.context_id=i.context_id WHERE i.tenant_id=$1 AND i.version_id=$2)
OR EXISTS(SELECT 1 FROM decision_memory_items x WHERE x.tenant_id=$1 AND x.version_id=$2)
OR EXISTS(SELECT 1 FROM memory_attestations x WHERE x.tenant_id=$1 AND x.version_id=$2)
OR EXISTS(SELECT 1 FROM extraction_jobs j JOIN extraction_job_payloads p ON p.tenant_id=j.tenant_id AND p.job_id=j.id WHERE j.tenant_id=$1 AND j.memory_version_id=$2)
OR EXISTS(SELECT 1 FROM extraction_attempt_memory_inputs i JOIN extraction_attempt_payloads p ON p.tenant_id=i.tenant_id AND p.attempt_id=i.attempt_id WHERE i.tenant_id=$1 AND i.version_id=$2 AND i.payload_retained)
OR EXISTS(SELECT 1 FROM extraction_memory_proposals d JOIN extraction_result_payloads p ON p.tenant_id=d.tenant_id AND p.attempt_id=d.attempt_id WHERE d.tenant_id=$1 AND d.version_id=$2)
OR EXISTS(SELECT 1 FROM consumer_offsets c WHERE c.tenant_id=$1 AND c.consumer_id LIKE 'archive-%' AND c.acknowledged_revision < (
 SELECT max(e.ledger_revision) FROM memory_record_events r JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id WHERE r.tenant_id=$1 AND r.version_id=$2));

-- name: InsertFactVersionDisposal :exec
INSERT INTO fact_version_disposals(tenant_id,version_id,payload) VALUES($1,$2,$3);
-- name: InsertMemoryVersionDisposal :exec
INSERT INTO memory_version_disposals(tenant_id,version_id,payload) VALUES($1,$2,$3);
-- name: DeleteFactVersionPayload :execrows
DELETE FROM fact_version_payloads WHERE tenant_id=$1 AND version_id=$2;
-- name: DeleteMemoryVersionPayload :execrows
DELETE FROM memory_version_payloads WHERE tenant_id=$1 AND version_id=$2;
