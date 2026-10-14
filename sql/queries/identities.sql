-- name: GetScopeIdentity :one
SELECT v.payload
FROM scope_versions v
JOIN scopes s ON s.tenant_id=v.tenant_id AND s.id=v.scope_id
WHERE v.tenant_id=$1 AND v.scope_id=$2 AND v.revision=CASE WHEN sqlc.arg(revision)::bigint=0 THEN s.current_revision ELSE sqlc.arg(revision) END;

-- name: GetSubjectIdentity :one
SELECT v.payload
FROM subject_versions v
JOIN subjects s ON s.tenant_id=v.tenant_id AND s.scope_id=v.scope_id AND s.id=v.subject_id
WHERE v.tenant_id=$1 AND v.scope_id=$2 AND v.subject_id=$3 AND v.revision=CASE WHEN sqlc.arg(revision)::bigint=0 THEN s.current_revision ELSE sqlc.arg(revision) END;

-- name: LookupSubjectIdentity :one
SELECT subject_id
FROM subject_lookup
WHERE tenant_id=$1 AND scope_id=$2 AND namespace=$3 AND value_digest=$4;

-- name: EnsureScopeIdentity :exec
INSERT INTO scopes (tenant_id,id,current_revision)
VALUES ($1,$2,0)
ON CONFLICT DO NOTHING;

-- name: LockScopeIdentity :one
SELECT current_revision
FROM scopes
WHERE tenant_id=$1 AND id=$2
FOR UPDATE;

-- name: UpdateScopeIdentity :exec
UPDATE scopes SET current_revision=$3
WHERE tenant_id=$1 AND id=$2;

-- name: EnsureSubjectIdentity :exec
INSERT INTO subjects (tenant_id,scope_id,id,kind,current_revision)
VALUES ($1,$2,$3,$4,0)
ON CONFLICT DO NOTHING;

-- name: LockSubjectIdentity :one
SELECT current_revision,kind
FROM subjects
WHERE tenant_id=$1 AND scope_id=$2 AND id=$3
FOR UPDATE;

-- name: DeleteSubjectLookups :exec
DELETE
FROM subject_lookup
WHERE tenant_id=$1 AND scope_id=$2 AND subject_id=$3;

-- name: InsertSubjectLookup :exec
INSERT INTO subject_lookup (tenant_id,scope_id,namespace,value_digest,subject_id)
VALUES ($1,$2,$3,$4,$5);

-- name: UpdateSubjectIdentity :exec
UPDATE subjects SET current_revision=$4
WHERE tenant_id=$1 AND scope_id=$2 AND id=$3;

-- name: InsertScopeVersion :exec
INSERT INTO scope_versions (tenant_id,scope_id,revision,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5);

-- name: InsertSubjectVersion :exec
INSERT INTO subject_versions (tenant_id,scope_id,subject_id,revision,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: ScopeIdentityExists :one
SELECT EXISTS(SELECT 1
FROM scopes
WHERE tenant_id=$1 AND id=$2 AND current_revision>0);

-- name: SubjectIdentityExists :one
SELECT EXISTS(SELECT 1
FROM subjects
WHERE tenant_id=$1 AND scope_id=$2 AND id=$3 AND current_revision>0);
