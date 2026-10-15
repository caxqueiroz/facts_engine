-- name: LockExtractionContentGuard :exec
INSERT INTO extraction_content_guards(tenant_id,job_id,epoch) VALUES($1,$2,1)
ON CONFLICT(tenant_id,job_id) DO UPDATE SET epoch=extraction_content_guards.epoch+1;

-- name: ExtractionDisposalJobHeader :one
SELECT j.scope_id,j.profile_digest,j.ledger_revision,e.recorded_at
FROM extraction_jobs j JOIN ledger_events e ON e.tenant_id=j.tenant_id AND e.ledger_revision=j.ledger_revision
WHERE j.tenant_id=$1 AND j.id=$2;

-- name: ExtractionDisposalAttempts :many
SELECT a.id,a.scope_id,a.fence,a.ledger_revision,e.recorded_at,
 r.ledger_revision AS result_revision,re.recorded_at AS result_time,
 i.inventory_version,i.request_digest,i.fact_count,i.memory_count
FROM extraction_attempts a
JOIN ledger_events e ON e.tenant_id=a.tenant_id AND e.ledger_revision=a.ledger_revision
LEFT JOIN extraction_results r ON r.tenant_id=a.tenant_id AND r.attempt_id=a.id
LEFT JOIN ledger_events re ON re.tenant_id=r.tenant_id AND re.ledger_revision=r.ledger_revision
LEFT JOIN extraction_attempt_inventories i ON i.tenant_id=a.tenant_id AND i.attempt_id=a.id
WHERE a.tenant_id=$1 AND a.job_id=$2
ORDER BY a.id COLLATE "C" LIMIT 1001;

-- name: ExtractionDisposalFactInputs :many
SELECT record_id,version_id,payload_retained FROM extraction_attempt_fact_inputs
WHERE tenant_id=$1 AND attempt_id=$2 ORDER BY version_id COLLATE "C" LIMIT 10001;

-- name: ExtractionDisposalMemoryInputs :many
SELECT record_id,version_id,payload_retained FROM extraction_attempt_memory_inputs
WHERE tenant_id=$1 AND attempt_id=$2 ORDER BY version_id COLLATE "C" LIMIT 10001;

-- name: ExtractionDisposalScopes :many
SELECT j.scope_id FROM extraction_jobs j WHERE j.tenant_id=$1 AND j.id=$2
UNION SELECT d.scope_id FROM extraction_memory_dependencies d WHERE d.tenant_id=$1 AND d.job_id=$2
UNION SELECT v.scope_id FROM extraction_jobs j JOIN memory_versions v ON v.tenant_id=j.tenant_id AND v.id=j.memory_version_id WHERE j.tenant_id=$1 AND j.id=$2
UNION SELECT d.scope_id FROM extraction_jobs j JOIN memory_dependencies d ON d.tenant_id=j.tenant_id AND d.memory_version_id=j.memory_version_id WHERE j.tenant_id=$1 AND j.id=$2
UNION SELECT v.scope_id FROM extraction_attempts a JOIN extraction_attempt_fact_inputs i ON i.tenant_id=a.tenant_id AND i.attempt_id=a.id JOIN record_versions v ON v.tenant_id=i.tenant_id AND v.id=i.version_id WHERE a.tenant_id=$1 AND a.job_id=$2
UNION SELECT v.scope_id FROM extraction_attempts a JOIN extraction_attempt_memory_inputs i ON i.tenant_id=a.tenant_id AND i.attempt_id=a.id JOIN memory_versions v ON v.tenant_id=i.tenant_id AND v.id=i.version_id WHERE a.tenant_id=$1 AND a.job_id=$2
UNION SELECT d.scope_id FROM extraction_attempts a JOIN extraction_attempt_memory_inputs i ON i.tenant_id=a.tenant_id AND i.attempt_id=a.id JOIN memory_dependencies d ON d.tenant_id=i.tenant_id AND d.memory_version_id=i.version_id WHERE a.tenant_id=$1 AND a.job_id=$2;

-- name: ExtractionDisposalBlocked :one
SELECT EXISTS(SELECT 1 FROM extraction_attempts a JOIN extraction_proposals p ON p.tenant_id=a.tenant_id AND p.attempt_id=a.id JOIN fact_version_payloads v ON v.tenant_id=p.tenant_id AND v.version_id=p.version_id WHERE a.tenant_id=sqlc.arg(tenant_id) AND a.job_id=sqlc.arg(job_id))
OR EXISTS(SELECT 1 FROM extraction_attempts a JOIN extraction_memory_proposals p ON p.tenant_id=a.tenant_id AND p.attempt_id=a.id JOIN memory_version_payloads v ON v.tenant_id=p.tenant_id AND v.version_id=p.version_id WHERE a.tenant_id=sqlc.arg(tenant_id) AND a.job_id=sqlc.arg(job_id))
OR EXISTS(SELECT 1 FROM audit_export_requests WHERE tenant_id=sqlc.arg(tenant_id) AND scope_id=ANY(sqlc.arg(scopes)::text[]))
OR EXISTS(SELECT 1 FROM consumer_offsets WHERE tenant_id=sqlc.arg(tenant_id) AND consumer_id LIKE 'archive-%' AND acknowledged_revision<sqlc.arg(required_revision)::bigint);

-- name: InsertExtractionJobDisposal :exec
INSERT INTO extraction_job_disposals(tenant_id,job_id,payload) VALUES($1,$2,$3);
-- name: InsertExtractionAttemptDisposal :exec
INSERT INTO extraction_attempt_disposals(tenant_id,attempt_id,payload) VALUES($1,$2,$3);
-- name: InsertExtractionResultDisposal :exec
INSERT INTO extraction_result_disposals(tenant_id,attempt_id,payload) VALUES($1,$2,$3);
-- name: DeleteExtractionJobPayload :execrows
DELETE FROM extraction_job_payloads WHERE tenant_id=$1 AND job_id=$2;
-- name: DeleteExtractionAttemptPayload :execrows
DELETE FROM extraction_attempt_payloads WHERE tenant_id=$1 AND attempt_id=$2;
-- name: DeleteExtractionResultPayload :execrows
DELETE FROM extraction_result_payloads WHERE tenant_id=$1 AND attempt_id=$2;
