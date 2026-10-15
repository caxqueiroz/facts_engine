-- name: LockCommandReceipt :exec
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(lock_key)::text,0));

-- name: GetCommandReceipt :one
SELECT digest,payload
FROM command_receipts
WHERE tenant_id=$1 AND principal=$2 AND operation=$3 AND command_key=$4;

-- name: InsertCommandReceipt :exec
INSERT INTO command_receipts (tenant_id,principal,operation,command_key,digest,payload)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: GetPolicyPayload :one
SELECT payload
FROM policy_versions
WHERE tenant_id=$1 AND scope_id=$2 AND id=$3;

-- name: InsertPolicyPayload :exec
INSERT INTO policy_versions (tenant_id,scope_id,id,payload)
VALUES ($1,$2,$3,$4);

-- name: GetPredicatePayload :one
SELECT payload
FROM predicate_versions
WHERE tenant_id=$1 AND scope_id=$2 AND predicate=$3 AND version=$4;

-- name: InsertPredicatePayload :exec
INSERT INTO predicate_versions (tenant_id,scope_id,predicate,version,policy_id,payload)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: InsertPredicateMigration :exec
INSERT INTO predicate_migrations (tenant_id,scope_id,predicate,version,from_version,mode)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: SetActivePredicate :exec
INSERT INTO active_predicates (tenant_id,scope_id,predicate,version)
VALUES ($1,$2,$3,$4)
ON CONFLICT(tenant_id,scope_id,predicate) DO UPDATE SET version=EXCLUDED.version;

-- name: GetEvidencePayload :one
SELECT payload
FROM evidence
WHERE tenant_id=$1 AND id=$2;

-- name: FindEvidenceSource :one
SELECT id,digest
FROM evidence
WHERE tenant_id=$1 AND scope_id=$2 AND source_identity=$3;

-- name: InsertEvidencePayload :exec
INSERT INTO evidence (tenant_id,id,scope_id,source_identity,digest,payload)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: InsertEvidenceRetention :exec
INSERT INTO evidence_retention(tenant_id,evidence_id,scope_id,locator,object_version)
VALUES ($1,$2,$3,$4,$5);

-- name: InsertEvidenceDerivation :exec
INSERT INTO evidence_derivations(tenant_id,scope_id,evidence_id,parent_evidence_id)
VALUES ($1,$2,$3,$4);

-- name: GetRecord :one
SELECT id,scope_id,subject_id,predicate,discriminator,stream_revision,kind
FROM records
WHERE tenant_id=$1 AND id=$2;

-- name: EnsureFactRecord :exec
INSERT INTO records(tenant_id,id,scope_id,subject_id,predicate,discriminator,stream_revision)
VALUES ($1,$2,$3,$4,$5,$6,0)
ON CONFLICT DO NOTHING;

-- name: GetVersionPayload :one
SELECT p.payload
FROM record_versions v
LEFT JOIN fact_version_payloads p ON p.tenant_id=v.tenant_id AND p.version_id=v.id
WHERE v.tenant_id=$1 AND v.record_id=$2 AND v.id=$3;

-- name: InsertVersionHeader :exec
INSERT INTO record_versions (tenant_id,record_id,id,scope_id,predicate,schema_version,digest)
VALUES ($1,$2,$3,$4,$5,$6,$7);

-- name: InsertVersionPayload :exec
INSERT INTO fact_version_payloads(tenant_id,version_id,payload) VALUES($1,$2,$3);

-- name: InsertVersionEvidence :exec
INSERT INTO version_evidence (tenant_id,version_id,evidence_id,scope_id)
VALUES ($1,$2,$3,$4)
ON CONFLICT DO NOTHING;

-- name: GetConversationPayload :one
SELECT payload
FROM conversations
WHERE tenant_id=$1 AND id=$2;

-- name: InsertConversationPayload :exec
INSERT INTO conversations (tenant_id,id,scope_id,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5);

-- name: InsertConversationMessage :exec
INSERT INTO conversation_messages (tenant_id,conversation_id,ordinal,evidence_id)
VALUES ($1,$2,$3,$4);

-- name: InsertAttestationDelegation :exec
INSERT INTO attestation_delegations (tenant_id,authority,binding_id,request_digest,attestation_id,scope_id,kind)
VALUES ($1,$2,$3,$4,$5,$6,$7);

-- name: InsertConfirmationArtifact :exec
INSERT INTO confirmation_artifacts (tenant_id,attestation_id,scope_id,kind,role,evidence_id,digest,object_locator,object_version)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9);
