-- name: GetEventAtRevision :one
SELECT payload
FROM ledger_events
WHERE tenant_id=$1 AND id=$2 AND ledger_revision=$3;

-- name: ListMemoryRefreshDependents :many
SELECT DISTINCT v.record_id,v.id
FROM memory_versions v
JOIN memory_dependencies d ON d.tenant_id=v.tenant_id AND d.memory_version_id=v.id
WHERE v.tenant_id=$1 AND v.scope_id=$2 AND ($3::text='' OR d.record_id=$3) AND v.ledger_revision<=$4 AND v.id>$5 AND EXISTS(SELECT 1
FROM memory_timeline_history h
WHERE h.tenant_id=v.tenant_id AND h.record_id=v.record_id AND h.version_id=v.id AND h.ledger_revision=(SELECT max(g.ledger_revision)
FROM memory_timeline_generations g
WHERE g.tenant_id=v.tenant_id AND g.record_id=v.record_id AND g.ledger_revision<=$4)) ORDER BY v.id,v.record_id LIMIT $6;

-- name: GetRevisionAt :one
SELECT coalesce(max(ledger_revision),0)::bigint
FROM ledger_events
WHERE tenant_id=$1 AND recorded_at<=$2;

-- name: ListFactRecords :many
SELECT r.id,r.scope_id,r.subject_id,r.predicate,r.discriminator
FROM records r
WHERE r.kind='fact' AND r.tenant_id=$1 AND r.scope_id=$2 AND ($3::text='' OR r.subject_id=$3) AND ($4::text='' OR r.predicate=$4) AND r.id>$5 AND EXISTS(SELECT 1
FROM record_events re
JOIN ledger_events e ON e.tenant_id=re.tenant_id AND e.id=re.event_id
WHERE re.tenant_id=r.tenant_id AND re.record_id=r.id AND e.ledger_revision<=$6) ORDER BY r.id LIMIT $7;

-- name: ListMemoryRecords :many
SELECT r.id,r.scope_id,r.discriminator
FROM records r
WHERE r.kind='memory' AND r.tenant_id=$1 AND r.scope_id=$2 AND r.id>$3 AND EXISTS(SELECT 1
FROM memory_record_events re
JOIN ledger_events e ON e.tenant_id=re.tenant_id AND e.id=re.event_id
WHERE re.tenant_id=r.tenant_id AND re.record_id=r.id AND e.ledger_revision<=$4) AND EXISTS(SELECT 1
FROM memory_versions v
WHERE v.tenant_id=r.tenant_id AND v.record_id=r.id AND v.ledger_revision<=$4 AND ($5::text='' OR v.subtype=$5)) ORDER BY r.id LIMIT $6;

-- name: ListFactHistoryPage :many
SELECT e.payload,e.recorded_at
FROM record_events r
JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id
WHERE r.tenant_id=$1 AND r.record_id=$2 AND r.stream_revision>$3 AND e.ledger_revision<=$4 ORDER BY r.stream_revision LIMIT $5;

-- name: ListMemoryHistoryPage :many
SELECT e.payload,e.recorded_at
FROM memory_record_events r
JOIN ledger_events e ON e.tenant_id=r.tenant_id AND e.id=r.event_id
WHERE r.tenant_id=$1 AND r.record_id=$2 AND r.stream_revision>$3 AND e.ledger_revision<=$4 ORDER BY r.stream_revision LIMIT $5;
