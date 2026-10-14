-- name: RegisterConsumer :execrows
INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts)
VALUES ($1,$2,$3)
ON CONFLICT DO NOTHING;

-- name: GetConsumerMaxAttempts :one
SELECT max_attempts
FROM consumer_offsets
WHERE tenant_id=$1 AND consumer_id=$2;

-- name: LockAvailableConsumer :one
SELECT acknowledged_revision,fence,attempts,max_attempts
FROM consumer_offsets
WHERE tenant_id=$1 AND consumer_id=$2 AND ((state='pending' AND available_at<=clock_timestamp()) OR (state='running' AND lease_until<=clock_timestamp())) FOR UPDATE SKIP LOCKED;

-- name: FailExhaustedConsumer :execrows
UPDATE consumer_offsets SET state='failed',lease_until=NULL,last_failure_code='lease_expired'
WHERE tenant_id=$1 AND consumer_id=$2;

-- name: GetNextOutboxEvent :one
SELECT e.payload,e.recorded_at
FROM outbox o
JOIN ledger_events e ON e.tenant_id=o.tenant_id AND e.id=o.event_id
WHERE o.tenant_id=$1 AND o.ledger_revision=$2;

-- name: ClaimConsumerDelivery :one
UPDATE consumer_offsets SET state='running',fence=fence+1,attempts=attempts+1,lease_until=clock_timestamp()+($3::bigint*interval '1 millisecond')
WHERE tenant_id=$1 AND consumer_id=$2 RETURNING lease_until;

-- name: RenewConsumerDelivery :execrows
UPDATE consumer_offsets AS c SET lease_until=clock_timestamp()+($6::bigint*interval '1 millisecond')
WHERE c.tenant_id=$1 AND c.consumer_id=$2 AND c.fence=$3 AND c.acknowledged_revision=$4::bigint-1 AND c.state='running' AND c.lease_until>clock_timestamp() AND EXISTS(SELECT 1
FROM outbox o
WHERE o.tenant_id=$1 AND o.event_id=$5 AND o.ledger_revision=$4);

-- name: AcknowledgeConsumerDelivery :execrows
UPDATE consumer_offsets AS c SET acknowledged_revision=$4,state='pending',attempts=0,lease_until=NULL,available_at=clock_timestamp(),last_failure_code=''
WHERE c.tenant_id=$1 AND c.consumer_id=$2 AND c.fence=$3 AND c.acknowledged_revision=$4::bigint-1 AND c.state='running' AND c.lease_until>clock_timestamp() AND EXISTS(SELECT 1
FROM outbox o
WHERE o.tenant_id=$1 AND o.event_id=$5 AND o.ledger_revision=$4) AND $6::bigint=0;

-- name: FailConsumerDelivery :execrows
UPDATE consumer_offsets AS c SET state=CASE WHEN attempts>=max_attempts THEN 'failed' ELSE 'pending' END,lease_until=NULL,available_at=clock_timestamp()+($6::bigint*interval '1 millisecond'),last_failure_code='handler_failed'
WHERE c.tenant_id=$1 AND c.consumer_id=$2 AND c.fence=$3 AND c.acknowledged_revision=$4::bigint-1 AND c.state='running' AND c.lease_until>clock_timestamp() AND EXISTS(SELECT 1
FROM outbox o
WHERE o.tenant_id=$1 AND o.event_id=$5 AND o.ledger_revision=$4);

-- name: GetConsumerStatus :one
SELECT acknowledged_revision,state,fence,attempts,max_attempts,lease_until,available_at,last_failure_code
FROM consumer_offsets
WHERE tenant_id=$1 AND consumer_id=$2;

-- name: HasConsumerRetry :one
SELECT EXISTS(SELECT 1
FROM consumer_retry_commands
WHERE tenant_id=$1 AND consumer_id=$2 AND failed_fence=$3);

-- name: RetryFailedConsumer :execrows
UPDATE consumer_offsets SET state='pending',attempts=0,lease_until=NULL,available_at=clock_timestamp(),fence=fence+1
WHERE tenant_id=$1 AND consumer_id=$2 AND state='failed' AND fence=$3;

-- name: InsertConsumerRetry :execrows
INSERT INTO consumer_retry_commands(tenant_id,consumer_id,failed_fence)
VALUES ($1,$2,$3);

-- name: ListScopeChanges :many
SELECT payload,recorded_at
FROM ledger_events
WHERE tenant_id=$1 AND scope_id=$2 AND ledger_revision>$3 AND ledger_revision<=$4 ORDER BY ledger_revision LIMIT $5;

-- name: ReadOutboxProgress :one
SELECT COALESCE(r.revision,0)::bigint AS ledger_revision,c.acknowledged_revision,c.state,e.recorded_at
FROM consumer_offsets c
LEFT JOIN tenant_revisions r ON r.tenant_id=c.tenant_id
LEFT JOIN outbox o ON o.tenant_id=c.tenant_id AND o.ledger_revision=c.acknowledged_revision+1
LEFT JOIN ledger_events e ON e.tenant_id=o.tenant_id AND e.id=o.event_id
WHERE c.tenant_id=$1 AND c.consumer_id=$2;
