-- name: LockAttestationGuard :exec
INSERT INTO attestation_guards(tenant_id,attestation_id,epoch) VALUES($1,$2,1)
ON CONFLICT(tenant_id,attestation_id) DO UPDATE SET epoch=attestation_guards.epoch+1;

-- name: FactAttestationDisposalHeader :one
SELECT a.scope_id,a.record_id,a.version_id,a.ledger_revision,e.recorded_at
FROM attestations a JOIN ledger_events e ON e.tenant_id=a.tenant_id AND e.ledger_revision=a.ledger_revision
WHERE a.tenant_id=$1 AND a.id=$2;

-- name: MemoryAttestationDisposalHeader :one
SELECT a.scope_id,a.record_id,a.version_id,a.ledger_revision,e.recorded_at
FROM memory_attestations a JOIN ledger_events e ON e.tenant_id=a.tenant_id AND e.ledger_revision=a.ledger_revision
WHERE a.tenant_id=$1 AND a.id=$2;

-- name: AttestationDisposalBlocked :one
SELECT EXISTS(SELECT 1 FROM audit_export_requests aer WHERE aer.tenant_id=sqlc.arg(tenant_id)::text AND aer.scope_id=sqlc.arg(scope_id)::text)
OR EXISTS(SELECT 1 FROM consumer_offsets co WHERE co.tenant_id=sqlc.arg(tenant_id)::text AND co.consumer_id LIKE 'archive-%' AND co.acknowledged_revision<sqlc.arg(ledger_revision)::bigint);

-- name: InsertFactAttestationDisposal :exec
INSERT INTO fact_attestation_disposals(tenant_id,attestation_id,payload) VALUES($1,$2,$3);

-- name: InsertMemoryAttestationDisposal :exec
INSERT INTO memory_attestation_disposals(tenant_id,attestation_id,payload) VALUES($1,$2,$3);

-- name: DeleteFactAttestationPayload :execrows
DELETE FROM fact_attestation_payloads WHERE tenant_id=$1 AND attestation_id=$2;

-- name: DeleteMemoryAttestationPayload :execrows
DELETE FROM memory_attestation_payloads WHERE tenant_id=$1 AND attestation_id=$2;
