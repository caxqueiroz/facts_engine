-- name: GetProjectionGeneration :one
SELECT state,fence,through_revision,base_epoch,previous_generation,entry_count,digest,verification_version
FROM projection_generations
WHERE tenant_id=$1 AND generation=$2;

-- name: GetActiveProjection :one
SELECT generation,epoch
FROM active_projections
WHERE tenant_id=$1;

-- name: LockProjectionManagement :exec
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(lock_key)::text,0));

-- name: UpdateProjectionGeneration :execrows
UPDATE projection_generations SET state=$3,fence=$4,through_revision=$5,base_epoch=$6,previous_generation=$7,entry_count=$8,digest=$9,verification_version=$10
WHERE tenant_id=$1 AND generation=$2;

-- name: InsertProjectionRebuildEvent :execrows
INSERT INTO projection_rebuild_events(tenant_id,generation,fence,operation,payload)
VALUES ($1,$2,$3,$4,$5);

-- name: EnsureProjectionTenant :execrows
INSERT INTO tenant_revisions(tenant_id)
VALUES ($1)
ON CONFLICT DO NOTHING;

-- name: InsertProjectionGeneration :execrows
INSERT INTO projection_generations(tenant_id,generation,state,fence,base_epoch,previous_generation)
VALUES ($1,$2,'building',1,$3,$4);

-- name: LockProjectionConsumer :one
SELECT state,acknowledged_revision
FROM consumer_offsets
WHERE tenant_id=$1 AND consumer_id=$2 FOR UPDATE;

-- name: LockTenantRevision :one
SELECT revision
FROM tenant_revisions
WHERE tenant_id=$1 FOR UPDATE;

-- name: PauseProjectionConsumer :execrows
UPDATE consumer_offsets SET state='paused',fence=fence+1,lease_until=NULL
WHERE tenant_id=$1 AND consumer_id=$2;

-- name: ResumeProjectionConsumer :execrows
UPDATE consumer_offsets SET state='pending',available_at=clock_timestamp()
WHERE tenant_id=$1 AND consumer_id=$2 AND state='paused';

-- name: SetActiveProjection :execrows
INSERT INTO active_projections (tenant_id,generation,epoch)
VALUES ($1,$2,$3)
ON CONFLICT(tenant_id) DO UPDATE SET generation=EXCLUDED.generation,epoch=EXCLUDED.epoch;

-- name: ListLegacyProjectionEvents :many
SELECT DISTINCT ON (r.kind,r.version_id) e.payload
FROM ledger_events e
JOIN (SELECT tenant_id,event_id,version_id,'fact' AS kind
FROM record_events UNION ALL SELECT tenant_id,event_id,version_id,'memory' AS kind
FROM memory_record_events) r ON r.tenant_id=e.tenant_id AND r.event_id=e.id
WHERE e.tenant_id=$1 AND e.ledger_revision<=$2 AND e.event_type IN ('approve_fact','accept_fact','supersede_fact','approve_memory','accept_memory','supersede_memory') ORDER BY r.kind,r.version_id,e.ledger_revision LIMIT 10001;

-- name: ListProjectionEventPage :many
SELECT DISTINCT ON (r.kind COLLATE "C",r.version_id COLLATE "C") e.payload
FROM ledger_events e
JOIN (SELECT tenant_id,event_id,version_id,'fact' AS kind
FROM record_events UNION ALL SELECT tenant_id,event_id,version_id,'memory' AS kind
FROM memory_record_events) r ON r.tenant_id=e.tenant_id AND r.event_id=e.id
WHERE e.tenant_id=$1 AND e.ledger_revision<=$2 AND e.event_type IN ('approve_fact','accept_fact','supersede_fact','approve_memory','accept_memory','supersede_memory') AND (r.kind COLLATE "C",r.version_id COLLATE "C")>($3::text,$4::text) ORDER BY r.kind COLLATE "C",r.version_id COLLATE "C",e.ledger_revision LIMIT $5;
