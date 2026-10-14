-- Read-only migration-owner inventory. IDs are not verified labels or kinds.
-- Run against the application database; review each tenant separately.
WITH referenced AS (
 SELECT tenant_id,scope_id FROM ledger_events UNION
 SELECT tenant_id,scope_id FROM records UNION
 SELECT tenant_id,scope_id FROM policy_versions UNION
 SELECT tenant_id,scope_id FROM evidence UNION
 SELECT tenant_id,scope_id FROM conversations UNION
 SELECT tenant_id,scope_id FROM extraction_jobs UNION
 SELECT tenant_id,scope_id FROM retention_scopes UNION
 SELECT tenant_id,scope_id FROM context_scopes UNION
 SELECT tenant_id,scope_id FROM decision_scopes UNION
 SELECT tenant_id,scope_id FROM fact_snapshot_items
)
SELECT r.tenant_id,r.scope_id FROM referenced r
WHERE NOT EXISTS(SELECT 1 FROM scopes s WHERE s.tenant_id=r.tenant_id AND s.id=r.scope_id)
ORDER BY r.tenant_id,r.scope_id;

SELECT DISTINCT r.tenant_id,r.scope_id,r.subject_id FROM records r
WHERE r.kind='fact' AND NOT EXISTS(SELECT 1 FROM subjects s WHERE s.tenant_id=r.tenant_id AND s.scope_id=r.scope_id AND s.id=r.subject_id)
ORDER BY r.tenant_id,r.scope_id,r.subject_id;

-- Pre-registry job subjects are encrypted in job payloads; these jobs require
-- application review. Execution revalidates their declared subjects before the
-- provider call. Memory-only jobs legitimately have no extraction_subjects rows.
SELECT j.tenant_id,j.scope_id,j.id,s.state FROM extraction_jobs j
JOIN extraction_job_state s USING(tenant_id,id)
WHERE j.conversation_id IS NOT NULL AND s.state IN ('pending','running')
AND NOT EXISTS(SELECT 1 FROM extraction_subjects b WHERE b.tenant_id=j.tenant_id AND b.job_id=j.id)
AND NOT EXISTS(SELECT 1 FROM extraction_memory_targets m WHERE m.tenant_id=j.tenant_id AND m.job_id=j.id)
ORDER BY j.tenant_id,j.scope_id,j.id;
