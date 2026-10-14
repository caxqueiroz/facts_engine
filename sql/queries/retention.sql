-- name: GetRetentionDomain :one
SELECT ('postgres:'||identity::text)::text AS domain
FROM retention_domain
WHERE id;

-- name: LockRetentionScope :execrows
INSERT INTO retention_scopes (tenant_id,scope_id,epoch)
VALUES ($1,$2,1)
ON CONFLICT(tenant_id,scope_id) DO UPDATE SET epoch=retention_scopes.epoch+1;

-- name: LockRetentionObject :one
INSERT INTO retention_objects(tenant_id,locator,object_version,epoch)
VALUES ($1,$2,$3,1)
ON CONFLICT(tenant_id,locator,object_version) DO UPDATE SET epoch=retention_objects.epoch+1 RETURNING disposed;

-- name: GetObjectDisposed :one
SELECT disposed
FROM retention_objects
WHERE tenant_id=$1 AND locator=$2 AND object_version=$3;

-- name: GetRetentionPolicyPayload :one
SELECT payload
FROM retention_policies
WHERE tenant_id=$1 AND scope_id=$2 AND id=$3;

-- name: GetEvidenceRetentionStatus :one
SELECT e.registered_at,o.disposed
FROM evidence_retention e
JOIN retention_objects o USING(tenant_id,locator,object_version)
WHERE e.tenant_id=$1 AND e.evidence_id=$2;

-- name: ListActiveLegalHolds :many
SELECT h.id
FROM legal_holds h
WHERE h.tenant_id=$1 AND h.scope_id=$2 AND NOT EXISTS(SELECT 1
FROM legal_hold_releases r
WHERE r.tenant_id=h.tenant_id AND r.scope_id=h.scope_id AND r.id=h.id) ORDER BY h.id;

-- name: HasExtractionRedactionReference :one
SELECT EXISTS(SELECT 1
FROM extraction_redactions
WHERE tenant_id=$1 AND evidence_id=$2);

-- name: HasEvidenceDerivative :one
SELECT EXISTS(SELECT 1
FROM evidence_derivations
WHERE tenant_id=$1 AND parent_evidence_id=$2);

-- name: HasConversationReference :one
SELECT EXISTS(SELECT 1
FROM conversation_messages
WHERE tenant_id=$1 AND evidence_id=$2);

-- name: HasMemoryEvidenceReference :one
SELECT EXISTS(SELECT 1
FROM memory_evidence
WHERE tenant_id=$1 AND evidence_id=$2);

-- name: HasFactEvidenceReference :one
SELECT EXISTS(SELECT 1
FROM version_evidence
WHERE tenant_id=$1 AND evidence_id=$2);

-- name: HasConfirmationReference :one
SELECT EXISTS(SELECT 1
FROM confirmation_artifacts
WHERE tenant_id=$1 AND evidence_id=$2);

-- name: HasMemoryAttestationReference :one
SELECT EXISTS(SELECT 1
FROM memory_attestations
WHERE tenant_id=$1 AND evidence_id=$2);

-- name: HasFactAttestationReference :one
SELECT EXISTS(SELECT 1
FROM attestations
WHERE tenant_id=$1 AND evidence_id=$2);

-- name: HasCalculationArtifactReference :one
SELECT EXISTS(SELECT 1
FROM calculation_artifacts
WHERE tenant_id=$1 AND evidence_id=$2);

-- name: HasDecisionArtifactReference :one
SELECT EXISTS(SELECT 1
FROM decision_artifacts
WHERE tenant_id=$1 AND evidence_id=$2);

-- name: HasAuditExportReference :one
SELECT EXISTS(SELECT 1
FROM audit_export_requests a
JOIN evidence e ON e.tenant_id=a.tenant_id AND e.scope_id=a.scope_id
WHERE e.tenant_id=$1 AND e.id=$2);

-- name: HasSharedEvidenceObject :one
SELECT EXISTS(SELECT 1
FROM evidence_retention a
JOIN evidence_retention b USING(tenant_id,locator,object_version)
WHERE a.tenant_id=$1 AND a.evidence_id=$2 AND b.evidence_id<>a.evidence_id);

-- name: HasUnboundLegacyEvidence :one
SELECT EXISTS(SELECT 1
FROM evidence e
WHERE e.tenant_id=$1 AND e.id<>$2 AND NOT EXISTS(SELECT 1
FROM evidence_retention b
WHERE b.tenant_id=e.tenant_id AND b.evidence_id=e.id));

-- name: EvidenceAgeMature :one
SELECT clock_timestamp()>=sqlc.arg(registered_at)::timestamptz+(sqlc.arg(minimum_age_seconds)::bigint*interval '1 second');

-- name: DeleteEvidenceObject :execrows
DELETE
FROM evidence_objects
WHERE tenant_id=$1 AND locator=$2 AND object_version=$3;

-- name: MarkEvidenceObjectDisposed :execrows
UPDATE retention_objects SET disposed=true
WHERE tenant_id=$1 AND locator=$2 AND object_version=$3;

-- name: InsertRetentionPolicyPayload :execrows
INSERT INTO retention_policies (tenant_id,scope_id,id,payload)
VALUES ($1,$2,$3,$4);

-- name: InsertLegalHold :execrows
INSERT INTO legal_holds (tenant_id,scope_id,id)
VALUES ($1,$2,$3);

-- name: InsertLegalHoldRelease :execrows
INSERT INTO legal_hold_releases (tenant_id,scope_id,id)
VALUES ($1,$2,$3);

-- name: InsertRetentionEvent :execrows
INSERT INTO retention_events (tenant_id,id,scope_id,kind,target_id,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5,$6,$7);
