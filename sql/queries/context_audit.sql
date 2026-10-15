-- name: GetContextPayload :one
SELECT p.payload
FROM context_manifests c
LEFT JOIN context_payloads p ON p.tenant_id=c.tenant_id AND p.context_id=c.id
WHERE c.tenant_id=$1 AND c.id=$2;

-- name: GetDecisionPayload :one
SELECT payload
FROM decision_receipts
WHERE tenant_id=$1 AND id=$2;

-- name: LockDecisionRun :exec
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(lock_key)::text,0));

-- name: GetDecisionRunPayload :one
SELECT payload
FROM decision_receipts
WHERE tenant_id=$1 AND application_id=$2 AND external_decision_id=$3;

-- name: InsertContextManifest :exec
INSERT INTO context_manifests (tenant_id,id,scope_id,selection_revision,ledger_revision)
VALUES ($1,$2,$3,$4,$5);

-- name: InsertContextPayload :exec
INSERT INTO context_payloads(tenant_id,context_id,payload) VALUES ($1,$2,$3);

-- name: InsertContextScope :exec
INSERT INTO context_scopes (tenant_id,context_id,scope_id)
VALUES ($1,$2,$3);

-- name: InsertContextMemoryItem :exec
INSERT INTO context_memory_items (tenant_id,context_id,ordinal,record_id,version_id,scope_id)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: InsertContextFactItem :exec
INSERT INTO context_items (tenant_id,context_id,ordinal,record_id,version_id,scope_id)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: InsertDecisionPayload :exec
INSERT INTO decision_receipts (tenant_id,id,scope_id,application_id,external_decision_id,request_digest,context_id,snapshot_id,calculation_id,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11);

-- name: InsertDecisionScope :exec
INSERT INTO decision_scopes (tenant_id,decision_id,scope_id)
VALUES ($1,$2,$3);

-- name: InsertDecisionMemoryItem :exec
INSERT INTO decision_memory_items (tenant_id,decision_id,role,ordinal,record_id,version_id,scope_id)
VALUES ($1,$2,$3,$4,$5,$6,$7);

-- name: InsertDecisionFactItem :exec
INSERT INTO decision_items (tenant_id,decision_id,role,ordinal,record_id,version_id,scope_id)
VALUES ($1,$2,$3,$4,$5,$6,$7);

-- name: InsertDecisionArtifact :exec
INSERT INTO decision_artifacts (tenant_id,decision_id,role,evidence_id)
VALUES ($1,$2,$3,$4);

-- name: GetCalculationPayload :one
SELECT payload
FROM calculation_receipts
WHERE tenant_id=$1 AND id=$2;

-- name: LockCalculationRun :exec
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(lock_key)::text,0));

-- name: GetCalculationRunPayload :one
SELECT payload
FROM calculation_receipts
WHERE tenant_id=$1 AND application_id=$2 AND external_run_id=$3;

-- name: GetAuditJob :one
SELECT r.payload,j.state,j.fence,j.lease_until
FROM audit_export_requests r
JOIN audit_export_jobs j ON j.tenant_id=r.tenant_id AND j.id=r.id
WHERE r.tenant_id=$1 AND r.id=$2;

-- name: ClaimAuditJob :one
UPDATE audit_export_jobs AS j SET state='running',fence=fence+1,lease_until=clock_timestamp()+(sqlc.arg(lease_millis)::bigint*interval '1 millisecond')
WHERE j.tenant_id=$1 AND j.id=(SELECT candidate.id
FROM audit_export_jobs candidate
WHERE candidate.tenant_id=$1 AND candidate.id=$2 AND (candidate.state='pending' OR (candidate.state='running' AND candidate.lease_until<clock_timestamp())) FOR UPDATE SKIP LOCKED) RETURNING id;

-- name: GetAuditExport :one
SELECT payload,digest,ledger_revision
FROM audit_exports
WHERE tenant_id=$1 AND id=$2;

-- name: InsertCalculationPayload :exec
INSERT INTO calculation_receipts (tenant_id,id,scope_id,snapshot_id,application_id,external_run_id,request_digest,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9);

-- name: InsertCalculationSnapshot :exec
INSERT INTO calculation_snapshots (tenant_id,calculation_id,snapshot_id)
VALUES ($1,$2,$3);

-- name: InsertCalculationArtifact :exec
INSERT INTO calculation_artifacts (tenant_id,calculation_id,evidence_id,role)
VALUES ($1,$2,$3,$4);

-- name: InsertAuditRequest :exec
INSERT INTO audit_export_requests (tenant_id,id,scope_id,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5);

-- name: InsertAuditJob :exec
INSERT INTO audit_export_jobs(tenant_id,id)
VALUES ($1,$2);

-- name: CompleteAuditJob :execrows
UPDATE audit_export_jobs SET state='completed',lease_until=NULL
WHERE tenant_id=$1 AND id=$2 AND state='running' AND fence=$3 AND lease_until>clock_timestamp();

-- name: InsertAuditExport :exec
INSERT INTO audit_exports (tenant_id,id,ledger_revision,digest,payload)
VALUES ($1,$2,$3,$4,$5);
