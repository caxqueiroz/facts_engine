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

-- name: ListCurrentFactAttestations :many
SELECT (CASE WHEN disposed.attestation_id IS NOT NULL THEN disposed.payload ELSE p.payload END)::bytea AS payload FROM attestations a LEFT JOIN fact_attestation_payloads p ON p.tenant_id=a.tenant_id AND p.attestation_id=a.id LEFT JOIN fact_attestation_disposals disposed ON disposed.tenant_id=a.tenant_id AND disposed.attestation_id=a.id WHERE a.tenant_id=$1 AND a.version_id=$2 AND NOT EXISTS(SELECT 1 FROM attestation_events e WHERE e.tenant_id=a.tenant_id AND e.attestation_id=a.id) ORDER BY a.id;

-- name: InsertFactAttestationIdentity :exec
INSERT INTO attestation_identities (tenant_id,id,scope_id,kind) VALUES($1,$2,$3,'fact');

-- name: GetAttestationKind :one
SELECT kind FROM attestation_identities WHERE tenant_id=$1 AND id=$2;

-- name: GetFactAttestationPayload :one
SELECT (CASE WHEN disposed.attestation_id IS NOT NULL THEN disposed.payload ELSE p.payload END)::bytea AS payload FROM attestations a LEFT JOIN fact_attestation_payloads p ON p.tenant_id=a.tenant_id AND p.attestation_id=a.id LEFT JOIN fact_attestation_disposals disposed ON disposed.tenant_id=a.tenant_id AND disposed.attestation_id=a.id WHERE a.tenant_id=$1 AND a.id=$2;

-- name: InsertLedgerEvent :one
INSERT INTO ledger_events(tenant_id,id,ledger_revision,event_type,scope_id,payload) VALUES($1,$2,$3,$4,$5,$6) RETURNING recorded_at;

-- name: InsertMemoryRecordEvent :exec
INSERT INTO memory_record_events (tenant_id,event_id,record_id,version_id,stream_revision) VALUES($1,$2,$3,$4,$5);

-- name: InsertFactRecordEvent :exec
INSERT INTO record_events (tenant_id,event_id,record_id,version_id,stream_revision) VALUES($1,$2,$3,$4,$5);

-- name: GetActivePredicate :one
SELECT version FROM active_predicates WHERE tenant_id=$1 AND scope_id=$2 AND predicate=$3 FOR SHARE;
