-- name: RegisterProjectionDestination :exec
INSERT INTO projection_destinations(tenant_id,generation,consumer_id) VALUES($1,$2,$3)
ON CONFLICT(tenant_id,generation) DO NOTHING;

-- name: ListProjectionDestinations :many
SELECT generation FROM projection_destinations WHERE tenant_id=$1 ORDER BY generation COLLATE "C" LIMIT 10001;

-- name: ListUnmappedProjectionConsumers :many
SELECT c.consumer_id FROM consumer_offsets c WHERE c.tenant_id=$1 AND c.consumer_id LIKE 'projection-%'
AND NOT EXISTS(SELECT 1 FROM projection_destinations d WHERE d.tenant_id=c.tenant_id AND d.consumer_id=c.consumer_id)
ORDER BY c.consumer_id COLLATE "C" LIMIT 10001;

-- name: ProjectionRetentionEpoch :one
SELECT coalesce((SELECT epoch FROM projection_retention_guards WHERE tenant_id=$1),0)::bigint;

-- name: LockProjectionRetentionGuard :exec
INSERT INTO projection_retention_guards(tenant_id,epoch) VALUES($1,1)
ON CONFLICT(tenant_id) DO UPDATE SET epoch=projection_retention_guards.epoch+1;
