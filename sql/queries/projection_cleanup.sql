-- name: GetProjectionCleanupJob :one
SELECT inventory_epoch,target_count,created_at FROM projection_cleanup_jobs
WHERE tenant_id=$1 AND kind=$2 AND version_id=$3;

-- name: ListProjectionCleanupTargets :many
SELECT t.generation,a.acknowledgement_id,a.completed_at
FROM projection_cleanup_targets t LEFT JOIN projection_cleanup_acknowledgements a USING(tenant_id,kind,version_id,generation)
WHERE t.tenant_id=$1 AND t.kind=$2 AND t.version_id=$3
ORDER BY t.generation COLLATE "C" LIMIT 10001;

-- name: HasPendingProjectionCleanup :one
SELECT EXISTS(SELECT 1 FROM projection_cleanup_targets t WHERE t.tenant_id=$1 AND t.generation=$2
AND NOT EXISTS(SELECT 1 FROM projection_cleanup_acknowledgements a WHERE a.tenant_id=t.tenant_id AND a.kind=t.kind AND a.version_id=t.version_id AND a.generation=t.generation));

-- name: GetCleanupDisposalEvent :one
SELECT scope_id,kind,target_id,ledger_revision,payload FROM retention_events WHERE tenant_id=$1 AND id=$2;

-- name: CanAcknowledgeProjectionCleanup :one
SELECT has_table_privilege(current_user,'public.projection_cleanup_acknowledgements','INSERT');

-- name: InsertProjectionCleanupAcknowledgement :exec
INSERT INTO projection_cleanup_acknowledgements(tenant_id,kind,version_id,generation,acknowledgement_id)
VALUES($1,$2,$3,$4,$5);

-- name: ScanProjectionCleanup :many
WITH pending AS MATERIALIZED (
 SELECT t.tenant_id,t.kind,t.version_id,t.generation FROM projection_cleanup_targets t
 WHERE t.tenant_id=sqlc.arg(tenant_id)
 AND (t.kind COLLATE "C",t.version_id COLLATE "C",t.generation COLLATE "C") >
 (sqlc.arg(after_kind)::text COLLATE "C",sqlc.arg(after_version)::text COLLATE "C",sqlc.arg(after_generation)::text COLLATE "C")
 AND NOT EXISTS(SELECT 1 FROM projection_cleanup_acknowledgements a WHERE a.tenant_id=t.tenant_id AND a.kind=t.kind AND a.version_id=t.version_id AND a.generation=t.generation)
 ORDER BY t.kind COLLATE "C",t.version_id COLLATE "C",t.generation COLLATE "C" LIMIT sqlc.arg(page_limit)
)
SELECT p.kind,p.version_id,p.generation,coalesce(e.id,'') AS event_id,coalesce(e.scope_id,'') AS scope_id,coalesce(e.ledger_revision,0)::bigint AS ledger_revision,
 CASE WHEN octet_length(e.payload)<=1048576 THEN e.payload ELSE ''::bytea END AS payload
FROM pending p LEFT JOIN retention_events e ON e.tenant_id=p.tenant_id AND e.target_id=p.version_id
AND e.kind IN ('dispose_fact_payload','dispose_memory_payload') AND e.kind='dispose_'||p.kind||'_payload'
ORDER BY p.kind COLLATE "C",p.version_id COLLATE "C",p.generation COLLATE "C";
