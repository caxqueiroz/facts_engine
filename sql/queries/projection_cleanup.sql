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
