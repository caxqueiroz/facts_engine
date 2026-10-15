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
FROM calculation_artifacts a JOIN calculation_payloads p ON p.tenant_id=a.tenant_id AND p.calculation_id=a.calculation_id
WHERE a.tenant_id=$1 AND a.evidence_id=$2);

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

-- name: HasUnselectedEvidenceDerivative :one
SELECT EXISTS(SELECT 1 FROM evidence_derivations
WHERE tenant_id=$1 AND parent_evidence_id=$2 AND NOT (evidence_id=ANY(sqlc.arg(selected_ids)::text[])));

-- name: HasUnselectedSharedEvidenceObject :one
SELECT EXISTS(SELECT 1
FROM evidence_retention a
JOIN evidence_retention b USING(tenant_id,locator,object_version)
WHERE a.tenant_id=$1 AND a.evidence_id=$2
AND NOT (b.evidence_id=ANY(sqlc.arg(selected_ids)::text[])));

-- name: HasRegisteredEvidenceObject :one
SELECT EXISTS(SELECT 1 FROM evidence_retention
WHERE tenant_id=$1 AND locator=$2 AND object_version=$3);

-- name: ScanOrphanEvidence :many
SELECT u.locator,u.object_version,u.byte_size
FROM pending_evidence_uploads u
JOIN retention_objects o USING(tenant_id,locator,object_version)
JOIN evidence_objects b USING(tenant_id,locator,object_version)
WHERE u.tenant_id=sqlc.arg(tenant_id) AND u.scope_id=sqlc.arg(scope_id) AND u.policy_id=sqlc.arg(policy_id)
AND NOT o.disposed
AND (u.locator COLLATE "C",u.object_version COLLATE "C")>(sqlc.arg(after_locator)::text COLLATE "C",sqlc.arg(after_version)::text COLLATE "C")
AND u.uploaded_at<=clock_timestamp()-(sqlc.arg(minimum_age_seconds)::bigint*interval '1 second')
AND NOT EXISTS(SELECT 1 FROM evidence_retention e WHERE e.tenant_id=u.tenant_id AND e.locator=u.locator AND e.object_version=u.object_version)
ORDER BY u.locator COLLATE "C",u.object_version COLLATE "C"
LIMIT sqlc.arg(page_limit);

-- name: HasIncompleteEvidenceInventory :one
SELECT EXISTS(SELECT 1 FROM evidence e WHERE e.tenant_id=$1
AND NOT EXISTS(SELECT 1 FROM evidence_retention b WHERE b.tenant_id=e.tenant_id AND b.evidence_id=e.id));

-- name: LockSnapshotGuard :exec
INSERT INTO snapshot_guards(tenant_id,snapshot_id,epoch) VALUES($1,$2,1)
ON CONFLICT(tenant_id,snapshot_id) DO UPDATE SET epoch=snapshot_guards.epoch+1;

-- name: SnapshotContentAvailable :one
SELECT EXISTS(SELECT 1 FROM fact_snapshot_payloads p WHERE p.tenant_id=$1 AND p.snapshot_id=$2)
AND NOT EXISTS(SELECT 1 FROM snapshot_disposals d WHERE d.tenant_id=$1 AND d.snapshot_id=$2);

-- name: SnapshotCreationTime :one
SELECT e.recorded_at FROM fact_snapshots s JOIN ledger_events e ON e.tenant_id=s.tenant_id AND e.ledger_revision=s.creation_revision
WHERE s.tenant_id=$1 AND s.id=$2;

-- name: SnapshotDisposalBlocked :one
SELECT EXISTS(SELECT 1 FROM calculation_snapshots c JOIN calculation_payloads p ON p.tenant_id=c.tenant_id AND p.calculation_id=c.calculation_id WHERE c.tenant_id=$1 AND c.snapshot_id=$2)
OR EXISTS(SELECT 1 FROM decision_receipts d WHERE d.tenant_id=$1 AND d.snapshot_id=$2)
OR EXISTS(SELECT 1 FROM audit_export_requests a JOIN fact_snapshot_items i ON i.tenant_id=a.tenant_id AND i.scope_id=a.scope_id WHERE i.tenant_id=$1 AND i.snapshot_id=$2)
OR EXISTS(SELECT 1 FROM consumer_offsets co JOIN fact_snapshots s ON s.tenant_id=co.tenant_id WHERE s.tenant_id=$1 AND s.id=$2 AND co.consumer_id LIKE 'archive-%' AND co.acknowledged_revision<s.creation_revision);

-- name: DeleteSnapshotPayload :execrows
DELETE FROM fact_snapshot_payloads WHERE tenant_id=$1 AND snapshot_id=$2;

-- name: InsertSnapshotDisposal :exec
INSERT INTO snapshot_disposals(tenant_id,snapshot_id,payload) VALUES($1,$2,$3);

-- name: SnapshotScopes :many
SELECT DISTINCT scope_id FROM fact_snapshot_items WHERE tenant_id=$1 AND snapshot_id=$2 ORDER BY scope_id;

-- name: LockContextGuard :exec
INSERT INTO context_guards(tenant_id,context_id,epoch) VALUES($1,$2,1)
ON CONFLICT(tenant_id,context_id) DO UPDATE SET epoch=context_guards.epoch+1;

-- name: ContextContentAvailable :one
SELECT EXISTS(SELECT 1 FROM context_payloads p WHERE p.tenant_id=$1 AND p.context_id=$2)
AND NOT EXISTS(SELECT 1 FROM context_disposals d WHERE d.tenant_id=$1 AND d.context_id=$2);

-- name: ContextCreationTime :one
SELECT e.recorded_at FROM context_manifests s JOIN ledger_events e ON e.tenant_id=s.tenant_id AND e.ledger_revision=s.ledger_revision
WHERE s.tenant_id=$1 AND s.id=$2;

-- name: ContextDisposalBlocked :one
SELECT EXISTS(SELECT 1 FROM decision_receipts d WHERE d.tenant_id=$1 AND d.context_id=$2)
OR EXISTS(SELECT 1 FROM audit_export_requests a JOIN context_scopes i ON i.tenant_id=a.tenant_id AND i.scope_id=a.scope_id WHERE i.tenant_id=$1 AND i.context_id=$2)
OR EXISTS(SELECT 1 FROM consumer_offsets co JOIN context_manifests s ON s.tenant_id=co.tenant_id WHERE s.tenant_id=$1 AND s.id=$2 AND co.consumer_id LIKE 'archive-%' AND co.acknowledged_revision<s.ledger_revision);

-- name: DeleteContextPayload :execrows
DELETE FROM context_payloads WHERE tenant_id=$1 AND context_id=$2;

-- name: InsertContextDisposal :exec
INSERT INTO context_disposals(tenant_id,context_id,payload) VALUES($1,$2,$3);

-- name: ContextScopes :many
SELECT DISTINCT scope_id FROM context_scopes WHERE tenant_id=$1 AND context_id=$2 ORDER BY scope_id;
