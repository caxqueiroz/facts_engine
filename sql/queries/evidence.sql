-- name: InsertEvidenceObject :exec
INSERT INTO evidence_objects (tenant_id,locator,object_version,payload)
VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING;

-- name: GetEvidenceObjectPayload :one
SELECT payload FROM evidence_objects
WHERE tenant_id=$1 AND locator=$2 AND object_version=$3;

-- name: GetPendingEvidenceUpload :one
SELECT scope_id,policy_id,byte_size,uploaded_at FROM pending_evidence_uploads
WHERE tenant_id=$1 AND locator=$2 AND object_version=$3;

-- name: InsertPendingEvidenceUpload :exec
INSERT INTO pending_evidence_uploads(tenant_id,locator,object_version,scope_id,policy_id,byte_size)
VALUES($1,$2,$3,$4,$5,$6);
