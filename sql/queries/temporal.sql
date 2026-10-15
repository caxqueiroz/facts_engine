-- name: InsertTimelineGeneration :exec
INSERT INTO timeline_generations (tenant_id,record_id,ledger_revision)
VALUES ($1,$2,$3);

-- name: DeleteCurrentTimeline :exec
DELETE
FROM accepted_intervals
WHERE tenant_id=$1 AND record_id=$2;

-- name: InsertTimelineEntry :exec
INSERT INTO timeline_history (tenant_id,record_id,ledger_revision,ordinal,version_id,payload)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: InsertAcceptedInterval :exec
INSERT INTO accepted_intervals(tenant_id,record_id,version_id,valid_from,valid_to,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5,$6,$7);

-- name: InsertFactRejection :exec
INSERT INTO rejected_versions (tenant_id,record_id,version_id,ledger_revision)
VALUES ($1,$2,$3,$4);

-- name: InsertFactAttestation :exec
INSERT INTO attestations(tenant_id,id,scope_id,record_id,version_id,evidence_id,ledger_revision)
VALUES ($1,$2,$3,$4,$5,$6,$7);

-- name: InsertFactAttestationPayload :exec
INSERT INTO fact_attestation_payloads(tenant_id,attestation_id,payload) VALUES($1,$2,$3);

-- name: InsertFactWithdrawal :exec
INSERT INTO attestation_events(tenant_id,attestation_id,actor,ledger_revision)
VALUES ($1,$2,$3,$4);

-- name: InsertFactSnapshot :exec
INSERT INTO fact_snapshots (tenant_id,id,selection_revision,creation_revision,digest)
VALUES ($1,$2,$3,$4,$5);

-- name: InsertFactSnapshotPayload :exec
INSERT INTO fact_snapshot_payloads(tenant_id,snapshot_id,payload)
VALUES ($1,$2,$3);

-- name: InsertFactSnapshotItem :exec
INSERT INTO fact_snapshot_items (tenant_id,snapshot_id,ordinal,scope_id,selector_digest,record_id,version_id)
VALUES ($1,$2,$3,$4,$5,$6,$7);

-- name: ListCurrentAcceptances :many
SELECT payload
FROM accepted_intervals
WHERE tenant_id=$1 AND record_id=$2 ORDER BY valid_from,segment_id;

-- name: ListHistoricalAcceptances :many
SELECT h.payload
FROM timeline_history h
WHERE h.tenant_id=$1 AND h.record_id=$2 AND h.ledger_revision=(SELECT max(g.ledger_revision)
FROM timeline_generations g
WHERE g.tenant_id=$1 AND g.record_id=$2 AND g.ledger_revision<=$3) ORDER BY h.ordinal;

-- name: FactRejected :one
SELECT EXISTS(SELECT 1
FROM rejected_versions
WHERE tenant_id=$1 AND record_id=$2 AND version_id=$3 AND (sqlc.arg(revision)::bigint=0 OR ledger_revision<=sqlc.arg(revision)));

-- name: FactHasAcceptance :one
SELECT EXISTS(SELECT 1
FROM timeline_history
WHERE tenant_id=$1 AND record_id=$2 AND version_id=$3);

-- name: ListFactVersionPayloads :many
SELECT coalesce(d.payload,p.payload)::bytea AS payload
FROM record_versions v
LEFT JOIN fact_version_payloads p ON p.tenant_id=v.tenant_id AND p.version_id=v.id
LEFT JOIN fact_version_disposals d ON d.tenant_id=v.tenant_id AND d.version_id=v.id
JOIN record_events r ON r.tenant_id=v.tenant_id AND r.record_id=v.record_id AND r.version_id=v.id
JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id
WHERE v.tenant_id=$1 AND v.record_id=$2 AND e.event_type='propose_fact' AND e.ledger_revision<=$3 ORDER BY e.ledger_revision;

-- name: HasUnorderedAttestations :one
SELECT EXISTS(SELECT 1
FROM attestations a
WHERE a.tenant_id=$1 AND a.version_id=$2 AND (a.ledger_revision IS NULL OR EXISTS(SELECT 1
FROM attestation_events e
WHERE e.tenant_id=a.tenant_id AND e.attestation_id=a.id AND e.ledger_revision IS NULL)));

-- name: ListHistoricalAttestationPayloads :many
SELECT p.payload
FROM attestations a LEFT JOIN fact_attestation_payloads p ON p.tenant_id=a.tenant_id AND p.attestation_id=a.id
WHERE a.tenant_id=$1 AND a.version_id=$2 AND a.ledger_revision<=$3 AND NOT EXISTS(SELECT 1
FROM attestation_events e
WHERE e.tenant_id=a.tenant_id AND e.attestation_id=a.id AND e.ledger_revision<=$3) ORDER BY a.id;

-- name: GetRevisionTime :one
SELECT recorded_at
FROM ledger_events
WHERE tenant_id=$1 AND ledger_revision=$2;

-- name: GetFactStreamRevisionAt :one
SELECT coalesce(max(r.stream_revision),0)::bigint
FROM record_events r
JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id
WHERE r.tenant_id=$1 AND r.record_id=$2 AND e.ledger_revision<=$3;

-- name: GetSnapshotPayload :one
SELECT coalesce(p.payload,d.payload)::bytea AS payload
FROM fact_snapshots s
LEFT JOIN fact_snapshot_payloads p ON p.tenant_id=s.tenant_id AND p.snapshot_id=s.id
LEFT JOIN snapshot_disposals d ON d.tenant_id=s.tenant_id AND d.snapshot_id=s.id
WHERE s.tenant_id=$1 AND s.id=$2;

-- name: ListFactHistoryPayloads :many
SELECT e.payload,e.recorded_at
FROM ledger_events e
JOIN record_events r ON r.tenant_id=e.tenant_id AND r.event_id=e.id
WHERE e.tenant_id=$1 AND r.record_id=$2 ORDER BY e.ledger_revision LIMIT 1001;

-- name: ListMemoryTimelinePayloads :many
SELECT h.payload
FROM memory_timeline_history h
WHERE h.tenant_id=$1 AND h.record_id=$2 AND h.ledger_revision=(SELECT max(g.ledger_revision)
FROM memory_timeline_generations g
WHERE g.tenant_id=$1 AND g.record_id=$2 AND (sqlc.arg(revision)::bigint=0 OR g.ledger_revision<=sqlc.arg(revision))) ORDER BY h.ordinal;

-- name: MemoryRejected :one
SELECT EXISTS(SELECT 1
FROM memory_rejections
WHERE tenant_id=$1 AND record_id=$2 AND version_id=$3 AND (sqlc.arg(revision)::bigint=0 OR ledger_revision<=sqlc.arg(revision)));

-- name: MemoryHasAcceptance :one
SELECT EXISTS(SELECT 1
FROM memory_timeline_history
WHERE tenant_id=$1 AND record_id=$2 AND version_id=$3);

-- name: GetMemoryStreamRevisionAt :one
SELECT coalesce(max(r.stream_revision),0)::bigint
FROM memory_record_events r
JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id
WHERE r.tenant_id=$1 AND r.record_id=$2 AND e.ledger_revision<=$3;

-- name: InsertMemoryTimelineGeneration :exec
INSERT INTO memory_timeline_generations (tenant_id,record_id,ledger_revision)
VALUES ($1,$2,$3);

-- name: InsertMemoryTimelineEntry :exec
INSERT INTO memory_timeline_history (tenant_id,record_id,ledger_revision,ordinal,version_id,valid_from,valid_to,payload)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8);

-- name: InsertMemoryRejection :exec
INSERT INTO memory_rejections (tenant_id,record_id,version_id,ledger_revision)
VALUES ($1,$2,$3,$4);
