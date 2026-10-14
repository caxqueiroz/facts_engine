-- name: BindTenant :exec
SELECT set_config('app.tenant_id', sqlc.arg(tenant_id)::text, true);

-- name: EnsureTenant :exec
INSERT INTO tenant_revisions (tenant_id, revision) VALUES ($1,0) ON CONFLICT DO NOTHING;

-- name: AllocateRevision :one
UPDATE tenant_revisions SET revision=revision+1 WHERE tenant_id=$1 RETURNING revision;

-- name: GetTenantRevision :one
SELECT revision FROM tenant_revisions WHERE tenant_id=$1;

-- name: UpdateStreamRevision :exec
UPDATE records SET stream_revision=$3 WHERE tenant_id=$1 AND id=$2;

-- name: LockRecord :one
SELECT stream_revision FROM records WHERE tenant_id=$1 AND id=$2 FOR UPDATE;

-- name: InsertOutbox :exec
INSERT INTO outbox (tenant_id,event_id,ledger_revision) VALUES ($1,$2,$3);
