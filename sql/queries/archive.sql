-- name: ListArchiveScopeIdentity :many
SELECT scope_id||'/'||revision::text,payload
FROM scope_versions
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveSubjectIdentity :many
SELECT subject_id||'/'||revision::text,payload
FROM subject_versions
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveMemoryTimeline :many
SELECT record_id||'/'||ordinal::text,payload
FROM memory_timeline_history
WHERE tenant_id=$1 AND ledger_revision=$2 ORDER BY record_id,ordinal;

-- name: ListArchiveMemoryType :many
SELECT subtype||'/'||version,payload
FROM memory_types
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveMemoryVersion :many
SELECT id,payload
FROM memory_versions
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveExtractionAttempt :many
SELECT id,payload
FROM extraction_attempts
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveExtractionResult :many
SELECT attempt_id,payload
FROM extraction_results
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveExtractionJob :many
SELECT id,payload
FROM extraction_jobs
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveExtractionRetry :many
SELECT id,payload
FROM extraction_retry_events
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveConversation :many
SELECT id,payload
FROM conversations
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveRetention :many
SELECT id,payload
FROM retention_events
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveContext :many
SELECT c.id,p.payload
FROM context_manifests c
LEFT JOIN context_payloads p ON p.tenant_id=c.tenant_id AND p.context_id=c.id
WHERE c.tenant_id=$1 AND c.ledger_revision=$2;

-- name: ListArchiveDecision :many
SELECT id,payload
FROM decision_receipts
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveSnapshot :many
SELECT s.id,coalesce(p.payload,d.payload)::bytea AS payload
FROM fact_snapshots s
LEFT JOIN fact_snapshot_payloads p ON p.tenant_id=s.tenant_id AND p.snapshot_id=s.id
LEFT JOIN snapshot_disposals d ON d.tenant_id=s.tenant_id AND d.snapshot_id=s.id
WHERE s.tenant_id=$1 AND s.creation_revision=$2;

-- name: ListArchiveCalculation :many
SELECT id,payload
FROM calculation_receipts
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveMemoryAttestation :many
SELECT a.id,a.payload
FROM memory_attestations a
WHERE a.tenant_id=$1 AND (a.ledger_revision=$2 OR EXISTS(SELECT 1
FROM memory_attestation_events w
WHERE w.tenant_id=a.tenant_id AND w.attestation_id=a.id AND w.ledger_revision=$2));

-- name: ListArchiveAttestation :many
SELECT id,payload
FROM attestations
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: ListArchiveTimeline :many
SELECT record_id||'/'||ordinal::text,payload
FROM timeline_history
WHERE tenant_id=$1 AND ledger_revision=$2 ORDER BY record_id,ordinal;

-- name: ListArchiveAuditExport :many
SELECT id,payload
FROM audit_exports
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: GetArchiveEvent :one
SELECT payload,recorded_at
FROM ledger_events
WHERE tenant_id=$1 AND id=$2 AND ledger_revision=$3;

-- name: ListAuditScopeIdentity :many
SELECT scope_id||'/'||revision::text,payload
FROM scope_versions
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY revision LIMIT 10001;

-- name: ListAuditSubjectIdentity :many
SELECT subject_id||'/'||revision::text,payload
FROM subject_versions
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY ledger_revision LIMIT 10001;

-- name: ListAuditMemoryTimeline :many
SELECT h.record_id||'/'||h.ledger_revision::text||'/'||h.ordinal::text,h.payload
FROM memory_timeline_history h
JOIN records r ON r.tenant_id=h.tenant_id AND r.id=h.record_id
WHERE h.tenant_id=$1 AND r.scope_id=$2 ORDER BY h.ledger_revision,h.record_id,h.ordinal LIMIT 10001;

-- name: ListAuditMemoryType :many
SELECT subtype||'/'||version,payload
FROM memory_types
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY ledger_revision LIMIT 10001;

-- name: ListAuditMemoryVersion :many
SELECT id,payload
FROM memory_versions
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY ledger_revision LIMIT 10001;

-- name: ListAuditContext :many
SELECT c.id,p.payload
FROM context_manifests c
LEFT JOIN context_payloads p ON p.tenant_id=c.tenant_id AND p.context_id=c.id
WHERE c.tenant_id=$1 AND EXISTS(SELECT 1
FROM context_scopes s
WHERE s.tenant_id=c.tenant_id AND s.context_id=c.id AND s.scope_id=$2) ORDER BY c.id LIMIT 10001;

-- name: ListAuditDecision :many
SELECT c.id,c.payload
FROM decision_receipts c
WHERE c.tenant_id=$1 AND EXISTS(SELECT 1
FROM decision_scopes s
WHERE s.tenant_id=c.tenant_id AND s.decision_id=c.id AND s.scope_id=$2) ORDER BY c.id LIMIT 10001;

-- name: ListAuditExtractionAttempt :many
SELECT a.id,a.payload
FROM extraction_attempts a
JOIN extraction_jobs j ON j.tenant_id=a.tenant_id AND j.id=a.job_id
WHERE a.tenant_id=$1 AND (j.scope_id=$2 OR EXISTS(SELECT 1
FROM extraction_memory_dependencies d
WHERE d.tenant_id=j.tenant_id AND d.job_id=j.id AND d.scope_id=$2) OR EXISTS(SELECT 1
FROM memory_dependencies d
WHERE d.tenant_id=j.tenant_id AND d.memory_version_id=j.memory_version_id AND d.scope_id=$2)) ORDER BY a.ledger_revision LIMIT 10001;

-- name: ListAuditExtractionResult :many
SELECT a.attempt_id,a.payload
FROM extraction_results a
JOIN extraction_jobs j ON j.tenant_id=a.tenant_id AND j.id=a.job_id
WHERE a.tenant_id=$1 AND (j.scope_id=$2 OR EXISTS(SELECT 1
FROM extraction_memory_dependencies d
WHERE d.tenant_id=j.tenant_id AND d.job_id=j.id AND d.scope_id=$2) OR EXISTS(SELECT 1
FROM memory_dependencies d
WHERE d.tenant_id=j.tenant_id AND d.memory_version_id=j.memory_version_id AND d.scope_id=$2)) ORDER BY a.ledger_revision LIMIT 10001;

-- name: ListAuditExtractionRetry :many
SELECT r.id,r.payload
FROM extraction_retry_events r
JOIN extraction_jobs j ON j.tenant_id=r.tenant_id AND j.id=r.job_id
WHERE r.tenant_id=$1 AND j.scope_id=$2 ORDER BY r.ledger_revision LIMIT 10001;

-- name: ListAuditExtractionJob :many
SELECT j.id,j.payload
FROM extraction_jobs j
WHERE j.tenant_id=$1 AND (j.scope_id=$2 OR EXISTS(SELECT 1
FROM extraction_memory_dependencies d
WHERE d.tenant_id=j.tenant_id AND d.job_id=j.id AND d.scope_id=$2) OR EXISTS(SELECT 1
FROM memory_dependencies d
WHERE d.tenant_id=j.tenant_id AND d.memory_version_id=j.memory_version_id AND d.scope_id=$2)) ORDER BY j.ledger_revision LIMIT 10001;

-- name: ListAuditConversation :many
SELECT id,payload
FROM conversations
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY ledger_revision LIMIT 10001;

-- name: ListAuditRetention :many
SELECT id,payload
FROM retention_events
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY ledger_revision LIMIT 10001;

-- name: ListAuditPolicy :many
SELECT id,payload
FROM policy_versions
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id LIMIT 10001;

-- name: ListAuditPredicate :many
SELECT predicate||'/'||version,payload
FROM predicate_versions
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY predicate,version LIMIT 10001;

-- name: ListAuditVersion :many
SELECT id,payload
FROM record_versions
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id LIMIT 10001;

-- name: ListAuditEvidence :many
SELECT id,payload
FROM evidence
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id LIMIT 10001;

-- name: ListAuditMemoryAttestation :many
SELECT id,payload
FROM memory_attestations
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id LIMIT 10001;

-- name: ListAuditAttestation :many
SELECT id,payload
FROM attestations
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id LIMIT 10001;

-- name: ListAuditSnapshot :many
SELECT s.id,coalesce(p.payload,d.payload)::bytea AS payload
FROM fact_snapshots s
LEFT JOIN fact_snapshot_payloads p ON p.tenant_id=s.tenant_id AND p.snapshot_id=s.id
LEFT JOIN snapshot_disposals d ON d.tenant_id=s.tenant_id AND d.snapshot_id=s.id
WHERE s.tenant_id=$1 AND EXISTS(SELECT 1
FROM fact_snapshot_items i
WHERE i.tenant_id=s.tenant_id AND i.snapshot_id=s.id AND i.scope_id=$2) ORDER BY s.id LIMIT 10001;

-- name: ListAuditCalculation :many
SELECT c.id,c.payload
FROM calculation_receipts c
WHERE c.tenant_id=$1 AND (c.scope_id=$2 OR EXISTS(SELECT 1
FROM calculation_snapshots cs
JOIN fact_snapshot_items i ON i.tenant_id=cs.tenant_id AND i.snapshot_id=cs.snapshot_id
WHERE cs.tenant_id=c.tenant_id AND cs.calculation_id=c.id AND i.scope_id=$2)) ORDER BY c.id LIMIT 10001;

-- name: ListAuditRecords :many
SELECT id,subject_id,predicate,discriminator,stream_revision,kind
FROM records
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id LIMIT 10001;

-- name: ListAuditEvents :many
SELECT payload,recorded_at
FROM ledger_events
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY ledger_revision LIMIT 10001;

-- name: ListAuditFactTimeline :many
SELECT h.record_id,h.ledger_revision,h.ordinal,h.payload
FROM timeline_history h
JOIN records r ON r.tenant_id=h.tenant_id AND r.id=h.record_id
WHERE h.tenant_id=$1 AND r.scope_id=$2 ORDER BY h.record_id,h.ledger_revision,h.ordinal LIMIT 10001;

-- name: ListAuditAttestationWithdrawals :many
SELECT e.attestation_id,e.actor,e.recorded_at,e.ledger_revision
FROM attestation_events e
JOIN attestations a ON a.tenant_id=e.tenant_id AND a.id=e.attestation_id
WHERE e.tenant_id=$1 AND a.scope_id=$2 ORDER BY e.attestation_id LIMIT 10001;
