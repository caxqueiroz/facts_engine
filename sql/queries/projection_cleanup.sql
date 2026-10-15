-- name: GetProjectionCleanupJob :one
SELECT inventory_epoch,target_count,created_at FROM projection_cleanup_jobs
WHERE tenant_id=$1 AND kind=$2 AND version_id=$3;

-- name: ListProjectionCleanupTargets :many
SELECT t.generation,a.acknowledgement_id,a.completed_at
FROM projection_cleanup_targets t LEFT JOIN projection_cleanup_acknowledgements a USING(tenant_id,kind,version_id,generation)
WHERE t.tenant_id=$1 AND t.kind=$2 AND t.version_id=$3
ORDER BY t.generation COLLATE "C" LIMIT 10001;
