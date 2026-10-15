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
SELECT v.id,coalesce(d.payload,p.payload)::bytea AS payload
FROM memory_versions v
LEFT JOIN memory_version_payloads p ON p.tenant_id=v.tenant_id AND p.version_id=v.id
LEFT JOIN memory_version_disposals d ON d.tenant_id=v.tenant_id AND d.version_id=v.id
WHERE v.tenant_id=$1 AND v.ledger_revision=$2;

-- name: ListArchiveExtractionAttempt :many
SELECT h.id,coalesce(d.payload,p.payload)::bytea AS payload
FROM extraction_attempts h
LEFT JOIN extraction_attempt_payloads p ON p.tenant_id=h.tenant_id AND p.attempt_id=h.id
LEFT JOIN extraction_attempt_disposals d ON d.tenant_id=h.tenant_id AND d.attempt_id=h.id
WHERE h.tenant_id=$1 AND h.ledger_revision=$2;

-- name: ListArchiveExtractionResult :many
SELECT h.attempt_id,coalesce(d.payload,p.payload)::bytea AS payload
FROM extraction_results h
LEFT JOIN extraction_result_payloads p ON p.tenant_id=h.tenant_id AND p.attempt_id=h.attempt_id
LEFT JOIN extraction_result_disposals d ON d.tenant_id=h.tenant_id AND d.attempt_id=h.attempt_id
WHERE h.tenant_id=$1 AND h.ledger_revision=$2;

-- name: ListArchiveExtractionJob :many
SELECT h.id,coalesce(d.payload,p.payload)::bytea AS payload
FROM extraction_jobs h
LEFT JOIN extraction_job_payloads p ON p.tenant_id=h.tenant_id AND p.job_id=h.id
LEFT JOIN extraction_job_disposals d ON d.tenant_id=h.tenant_id AND d.job_id=h.id
WHERE h.tenant_id=$1 AND h.ledger_revision=$2;

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
SELECT c.id,coalesce(p.payload,d.payload)::bytea AS payload
FROM context_manifests c
LEFT JOIN context_payloads p ON p.tenant_id=c.tenant_id AND p.context_id=c.id
LEFT JOIN context_disposals d ON d.tenant_id=c.tenant_id AND d.context_id=c.id
WHERE c.tenant_id=$1 AND c.ledger_revision=$2;

-- name: ListArchiveDecision :many
SELECT d.id,(CASE WHEN disposed.decision_id IS NOT NULL THEN disposed.payload ELSE p.payload END)::bytea AS payload
FROM decision_receipts d LEFT JOIN decision_payloads p ON p.tenant_id=d.tenant_id AND p.decision_id=d.id
LEFT JOIN decision_disposals disposed ON disposed.tenant_id=d.tenant_id AND disposed.decision_id=d.id
WHERE d.tenant_id=$1 AND d.ledger_revision=$2;

-- name: ListArchiveSnapshot :many
SELECT s.id,coalesce(p.payload,d.payload)::bytea AS payload
FROM fact_snapshots s
LEFT JOIN fact_snapshot_payloads p ON p.tenant_id=s.tenant_id AND p.snapshot_id=s.id
LEFT JOIN snapshot_disposals d ON d.tenant_id=s.tenant_id AND d.snapshot_id=s.id
WHERE s.tenant_id=$1 AND s.creation_revision=$2;

-- name: ListArchiveCalculation :many
SELECT c.id,(CASE WHEN d.calculation_id IS NOT NULL THEN d.payload ELSE p.payload END)::bytea AS payload
FROM calculation_receipts c LEFT JOIN calculation_payloads p ON p.tenant_id=c.tenant_id AND p.calculation_id=c.id
LEFT JOIN calculation_disposals d ON d.tenant_id=c.tenant_id AND d.calculation_id=c.id
WHERE c.tenant_id=$1 AND c.ledger_revision=$2;

-- name: ListArchiveMemoryAttestation :many
SELECT a.id,p.payload
FROM memory_attestations a LEFT JOIN memory_attestation_payloads p ON p.tenant_id=a.tenant_id AND p.attestation_id=a.id
WHERE a.tenant_id=$1 AND (a.ledger_revision=$2 OR EXISTS(SELECT 1
FROM memory_attestation_events w
WHERE w.tenant_id=a.tenant_id AND w.attestation_id=a.id AND w.ledger_revision=$2));

-- name: ListArchiveAttestation :many
SELECT a.id,p.payload
FROM attestations a LEFT JOIN fact_attestation_payloads p ON p.tenant_id=a.tenant_id AND p.attestation_id=a.id
WHERE a.tenant_id=$1 AND a.ledger_revision=$2;

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
SELECT v.id,coalesce(d.payload,p.payload)::bytea AS payload
FROM memory_versions v
LEFT JOIN memory_version_payloads p ON p.tenant_id=v.tenant_id AND p.version_id=v.id
LEFT JOIN memory_version_disposals d ON d.tenant_id=v.tenant_id AND d.version_id=v.id
WHERE v.tenant_id=$1 AND v.scope_id=$2 ORDER BY v.ledger_revision LIMIT 10001;

-- name: ListAuditContext :many
SELECT c.id,coalesce(p.payload,d.payload)::bytea AS payload
FROM context_manifests c
LEFT JOIN context_payloads p ON p.tenant_id=c.tenant_id AND p.context_id=c.id
LEFT JOIN context_disposals d ON d.tenant_id=c.tenant_id AND d.context_id=c.id
WHERE c.tenant_id=$1 AND EXISTS(SELECT 1
FROM context_scopes s
WHERE s.tenant_id=c.tenant_id AND s.context_id=c.id AND s.scope_id=$2) ORDER BY c.id LIMIT 10001;

-- name: ListAuditDecision :many
SELECT c.id,(CASE WHEN disposed.decision_id IS NOT NULL THEN disposed.payload ELSE p.payload END)::bytea AS payload
FROM decision_receipts c LEFT JOIN decision_payloads p ON p.tenant_id=c.tenant_id AND p.decision_id=c.id
LEFT JOIN decision_disposals disposed ON disposed.tenant_id=c.tenant_id AND disposed.decision_id=c.id
WHERE c.tenant_id=$1 AND EXISTS(SELECT 1
FROM decision_scopes s
WHERE s.tenant_id=c.tenant_id AND s.decision_id=c.id AND s.scope_id=$2) ORDER BY c.id LIMIT 10001;

-- name: ListAuditExtractionAttempt :many
SELECT a.id,coalesce(d.payload,p.payload)::bytea AS payload
FROM extraction_attempts a
LEFT JOIN extraction_attempt_payloads p ON p.tenant_id=a.tenant_id AND p.attempt_id=a.id
LEFT JOIN extraction_attempt_disposals d ON d.tenant_id=a.tenant_id AND d.attempt_id=a.id
JOIN extraction_jobs j ON j.tenant_id=a.tenant_id AND j.id=a.job_id
WHERE a.tenant_id=$1 AND (j.scope_id=$2 OR EXISTS(SELECT 1
FROM extraction_memory_dependencies d
WHERE d.tenant_id=j.tenant_id AND d.job_id=j.id AND d.scope_id=$2) OR EXISTS(SELECT 1
FROM memory_dependencies d
WHERE d.tenant_id=j.tenant_id AND d.memory_version_id=j.memory_version_id AND d.scope_id=$2)) ORDER BY a.ledger_revision LIMIT 10001;

-- name: ListAuditExtractionResult :many
SELECT a.attempt_id,coalesce(d.payload,p.payload)::bytea AS payload
FROM extraction_results a
LEFT JOIN extraction_result_payloads p ON p.tenant_id=a.tenant_id AND p.attempt_id=a.attempt_id
LEFT JOIN extraction_result_disposals d ON d.tenant_id=a.tenant_id AND d.attempt_id=a.attempt_id
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
SELECT j.id,coalesce(d.payload,p.payload)::bytea AS payload
FROM extraction_jobs j
LEFT JOIN extraction_job_payloads p ON p.tenant_id=j.tenant_id AND p.job_id=j.id
LEFT JOIN extraction_job_disposals d ON d.tenant_id=j.tenant_id AND d.job_id=j.id
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
SELECT v.id,coalesce(d.payload,p.payload)::bytea AS payload
FROM record_versions v
LEFT JOIN fact_version_payloads p ON p.tenant_id=v.tenant_id AND p.version_id=v.id
LEFT JOIN fact_version_disposals d ON d.tenant_id=v.tenant_id AND d.version_id=v.id
WHERE v.tenant_id=$1 AND v.scope_id=$2 ORDER BY v.id LIMIT 10001;

-- name: ListAuditEvidence :many
SELECT id,payload
FROM evidence
WHERE tenant_id=$1 AND scope_id=$2 ORDER BY id LIMIT 10001;

-- name: ListAuditMemoryAttestation :many
SELECT a.id,p.payload
FROM memory_attestations a LEFT JOIN memory_attestation_payloads p ON p.tenant_id=a.tenant_id AND p.attestation_id=a.id
WHERE a.tenant_id=$1 AND a.scope_id=$2 ORDER BY a.id LIMIT 10001;

-- name: ListAuditAttestation :many
SELECT a.id,p.payload
FROM attestations a LEFT JOIN fact_attestation_payloads p ON p.tenant_id=a.tenant_id AND p.attestation_id=a.id
WHERE a.tenant_id=$1 AND a.scope_id=$2 ORDER BY a.id LIMIT 10001;

-- name: ListAuditSnapshot :many
SELECT s.id,coalesce(p.payload,d.payload)::bytea AS payload
FROM fact_snapshots s
LEFT JOIN fact_snapshot_payloads p ON p.tenant_id=s.tenant_id AND p.snapshot_id=s.id
LEFT JOIN snapshot_disposals d ON d.tenant_id=s.tenant_id AND d.snapshot_id=s.id
WHERE s.tenant_id=$1 AND EXISTS(SELECT 1
FROM fact_snapshot_items i
WHERE i.tenant_id=s.tenant_id AND i.snapshot_id=s.id AND i.scope_id=$2) ORDER BY s.id LIMIT 10001;

-- name: ListAuditCalculation :many
SELECT c.id,(CASE WHEN d.calculation_id IS NOT NULL THEN d.payload ELSE p.payload END)::bytea AS payload
FROM calculation_receipts c LEFT JOIN calculation_payloads p ON p.tenant_id=c.tenant_id AND p.calculation_id=c.id
LEFT JOIN calculation_disposals d ON d.tenant_id=c.tenant_id AND d.calculation_id=c.id
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

-- name: ListArchiveExtractionContentJobs :many
SELECT j.id AS job_id FROM extraction_jobs j WHERE j.tenant_id=sqlc.arg(tenant_id) AND j.ledger_revision=sqlc.arg(ledger_revision)
UNION SELECT a.job_id FROM extraction_attempts a WHERE a.tenant_id=sqlc.arg(tenant_id) AND a.ledger_revision=sqlc.arg(ledger_revision)
UNION SELECT r.job_id FROM extraction_results r WHERE r.tenant_id=sqlc.arg(tenant_id) AND r.ledger_revision=sqlc.arg(ledger_revision)
ORDER BY job_id;
