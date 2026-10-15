-- name: GetMemoryTypePayload :one
SELECT payload
FROM memory_types
WHERE tenant_id=$1 AND scope_id=$2 AND subtype=$3 AND version=$4;

-- name: GetActiveMemoryType :one
SELECT version
FROM active_memory_types
WHERE tenant_id=$1 AND scope_id=$2 AND subtype=$3 FOR SHARE;

-- name: SetActiveMemoryType :exec
INSERT INTO active_memory_types (tenant_id,scope_id,subtype,version)
VALUES ($1,$2,$3,$4)
ON CONFLICT(tenant_id,scope_id,subtype) DO UPDATE SET version=EXCLUDED.version;

-- name: EnsureMemoryRecord :exec
INSERT INTO records(tenant_id,id,scope_id,subject_id,predicate,discriminator,kind)
VALUES ($1,$2,$3,'','',$4,'memory')
ON CONFLICT DO NOTHING;

-- name: LockMemoryRecord :one
SELECT stream_revision,kind
FROM records
WHERE tenant_id=$1 AND id=$2 FOR UPDATE;

-- name: GetMemoryVersionPayload :one
SELECT p.payload
FROM memory_versions v
LEFT JOIN memory_version_payloads p ON p.tenant_id=v.tenant_id AND p.version_id=v.id
WHERE v.tenant_id=$1 AND v.record_id=$2 AND v.id=$3;

-- name: InsertMemoryTypePayload :exec
INSERT INTO memory_types (tenant_id,scope_id,subtype,version,policy_id,from_version,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8);

-- name: InsertMemoryVersionHeader :exec
INSERT INTO memory_versions(tenant_id,id,record_id,scope_id,subtype,schema_version,ledger_revision)
VALUES ($1,$2,$3,$4,$5,$6,$7);

-- name: InsertMemoryVersionPayload :exec
INSERT INTO memory_version_payloads(tenant_id,version_id,payload) VALUES($1,$2,$3);

-- name: InsertMemoryEvidence :exec
INSERT INTO memory_evidence (tenant_id,scope_id,version_id,evidence_id)
VALUES ($1,$2,$3,$4);

-- name: InsertMemoryDependency :exec
INSERT INTO memory_dependencies (tenant_id,memory_version_id,scope_id,record_id,version_id)
VALUES ($1,$2,$3,$4,$5);

-- name: ListMemoryVersionPayloads :many
SELECT p.payload
FROM memory_versions v
LEFT JOIN memory_version_payloads p ON p.tenant_id=v.tenant_id AND p.version_id=v.id
WHERE v.tenant_id=$1 AND v.record_id=$2 AND v.ledger_revision<=$3 ORDER BY v.ledger_revision,v.id LIMIT 10001;

-- name: InsertMemoryAttestationIdentity :exec
INSERT INTO attestation_identities (tenant_id,id,scope_id,kind)
VALUES ($1,$2,$3,'memory');

-- name: ListMemoryAttestationPayloads :many
SELECT a.payload
FROM memory_attestations a
WHERE a.tenant_id=$1 AND a.version_id=$2 AND (sqlc.arg(revision)::bigint=0 OR a.ledger_revision<=sqlc.arg(revision)) AND NOT EXISTS(SELECT 1
FROM memory_attestation_events w
WHERE w.tenant_id=a.tenant_id AND w.attestation_id=a.id AND (sqlc.arg(revision)::bigint=0 OR w.ledger_revision<=sqlc.arg(revision))) ORDER BY a.id;

-- name: GetMemoryAttestationPayload :one
SELECT payload
FROM memory_attestations
WHERE tenant_id=$1 AND id=$2;

-- name: InsertMemoryAttestationPayload :exec
INSERT INTO memory_attestations(tenant_id,id,scope_id,record_id,version_id,evidence_id,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8);

-- name: InsertMemoryWithdrawal :exec
INSERT INTO memory_attestation_events (tenant_id,attestation_id,actor,ledger_revision)
VALUES ($1,$2,$3,$4);
