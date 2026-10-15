-- name: GetExtractionJobPayload :one
SELECT coalesce(d.payload,p.payload)::bytea AS payload
FROM extraction_jobs h
LEFT JOIN extraction_job_payloads p ON p.tenant_id=h.tenant_id AND p.job_id=h.id
LEFT JOIN extraction_job_disposals d ON d.tenant_id=h.tenant_id AND d.job_id=h.id
WHERE h.tenant_id=$1 AND h.id=$2;

-- name: GetExtractionJobStatus :one
SELECT j.scope_id,j.ledger_revision,s.state,s.fence,s.attempts,s.max_attempts,s.attempt_id,s.available_at,s.lease_until,s.last_failure_code
FROM extraction_jobs j
JOIN extraction_job_state s USING(tenant_id,id)
WHERE j.tenant_id=$1 AND j.id=$2;

-- name: RetryExtractionJob :execrows
UPDATE extraction_job_state SET state='pending',fence=fence+1,attempts=0,lease_until=NULL,available_at=clock_timestamp(),last_failure_code=''
WHERE tenant_id=$1 AND id=$2 AND state='failed' AND fence=$3;

-- name: InsertExtractionJobHeader :exec
INSERT INTO extraction_jobs(tenant_id,id,scope_id,conversation_id,profile_id,profile_digest,ledger_revision,memory_record_id,memory_version_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9);

-- name: InsertExtractionRedaction :exec
INSERT INTO extraction_redactions(tenant_id,job_id,scope_id,evidence_id)
VALUES ($1,$2,$3,$4);

-- name: InsertExtractionSubject :exec
INSERT INTO extraction_subjects (tenant_id,job_id,scope_id,subject_id)
VALUES ($1,$2,$3,$4);

-- name: InsertExtractionMemoryTarget :exec
INSERT INTO extraction_memory_targets (tenant_id,job_id,id,scope_id,subtype,schema_version)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: InsertExtractionMemoryDependency :exec
INSERT INTO extraction_memory_dependencies (tenant_id,job_id,target_id,scope_id,record_id,version_id)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: InsertExtractionRefreshTrigger :exec
INSERT INTO extraction_refresh_triggers (tenant_id,job_id,event_id)
VALUES ($1,$2,$3);

-- name: InsertExtractionJobState :exec
INSERT INTO extraction_job_state(tenant_id,id,max_attempts)
VALUES ($1,$2,$3);

-- name: InsertExtractionRetryEvent :exec
INSERT INTO extraction_retry_events (tenant_id,id,job_id,ledger_revision,payload)
VALUES ($1,$2,$3,$4,$5);

-- name: LockExtractionQuota :one
INSERT INTO extraction_quota(tenant_id,day,reserved_tokens,epoch)
VALUES ($1,(clock_timestamp() AT TIME ZONE 'UTC')::date,0,1)
ON CONFLICT(tenant_id) DO UPDATE SET epoch=extraction_quota.epoch+1,day=EXCLUDED.day,reserved_tokens=CASE WHEN extraction_quota.day=EXCLUDED.day THEN extraction_quota.reserved_tokens ELSE 0 END,reserved_cost_units=CASE WHEN extraction_quota.day=EXCLUDED.day THEN extraction_quota.reserved_cost_units ELSE 0 END RETURNING reserved_tokens,reserved_cost_units,cost_unit,to_char(day,'YYYY-MM-DD')::text AS quota_day;

-- name: CountActiveExtractions :one
SELECT count(*)
FROM extraction_job_state
WHERE tenant_id=$1 AND state='running' AND lease_until>clock_timestamp();

-- name: ClaimExtractionJob :one
UPDATE extraction_job_state SET state='running',fence=fence+1,attempts=attempts+1,attempt_id=$3,lease_until=clock_timestamp()+sqlc.arg(lease_millis)::bigint*interval '1 millisecond',last_failure_code=''
WHERE tenant_id=$1 AND id=$2 AND attempts<max_attempts AND ((state='pending' AND available_at<=clock_timestamp()) OR (state='running' AND lease_until<=clock_timestamp())) RETURNING id;

-- name: ReserveExtractionQuota :exec
UPDATE extraction_quota SET reserved_tokens=reserved_tokens+$2,reserved_cost_units=reserved_cost_units+$3,cost_unit=$4
WHERE tenant_id=$1;

-- name: GetExtractionAttemptPayload :one
SELECT coalesce(d.payload,p.payload)::bytea AS payload
FROM extraction_attempts h
LEFT JOIN extraction_attempt_payloads p ON p.tenant_id=h.tenant_id AND p.attempt_id=h.id
LEFT JOIN extraction_attempt_disposals d ON d.tenant_id=h.tenant_id AND d.attempt_id=h.id
WHERE h.tenant_id=$1 AND h.id=$2;

-- name: GetExtractionResultPayload :one
SELECT coalesce(d.payload,p.payload)::bytea AS payload
FROM extraction_results h
LEFT JOIN extraction_result_payloads p ON p.tenant_id=h.tenant_id AND p.attempt_id=h.attempt_id
LEFT JOIN extraction_result_disposals d ON d.tenant_id=h.tenant_id AND d.attempt_id=h.attempt_id
WHERE h.tenant_id=$1 AND h.attempt_id=$2;

-- name: FinishExtractionJob :execrows
UPDATE extraction_job_state SET state=CASE WHEN sqlc.arg(next_state)::text='pending' AND attempts>=max_attempts THEN 'failed' ELSE sqlc.arg(next_state)::text END,lease_until=NULL,last_failure_code=sqlc.arg(last_failure_code),available_at=clock_timestamp()+sqlc.arg(delay_millis)::bigint*interval '1 millisecond'
WHERE tenant_id=sqlc.arg(tenant_id) AND id=sqlc.arg(id) AND attempt_id=sqlc.arg(attempt_id) AND fence=sqlc.arg(fence) AND state='running' AND lease_until>clock_timestamp();

-- name: InsertExtractionAttemptHeader :exec
INSERT INTO extraction_attempts(tenant_id,id,job_id,scope_id,fence,ledger_revision)
VALUES ($1,$2,$3,$4,$5,$6);

-- name: InsertExtractionResultHeader :exec
INSERT INTO extraction_results(tenant_id,attempt_id,job_id,scope_id,ledger_revision)
VALUES ($1,$2,$3,$4,$5);

-- name: InsertExtractionMemoryProposal :exec
INSERT INTO extraction_memory_proposals (tenant_id,attempt_id,ordinal,record_id,version_id)
VALUES ($1,$2,$3,$4,$5);

-- name: InsertExtractionFactProposal :exec
INSERT INTO extraction_proposals (tenant_id,attempt_id,ordinal,record_id,version_id)
VALUES ($1,$2,$3,$4,$5);

-- name: FailExpiredExtraction :execrows
UPDATE extraction_job_state SET state='failed',lease_until=NULL,last_failure_code='lease_exhausted'
WHERE tenant_id=$1 AND id=$2 AND fence=$3 AND state='running' AND attempts>=max_attempts AND lease_until<=clock_timestamp();

-- name: RejectExtractionInput :execrows
UPDATE extraction_job_state SET state='failed',fence=fence+1,lease_until=NULL,last_failure_code='input_limit'
WHERE tenant_id=$1 AND id=$2 AND (state='pending' OR (state='running' AND lease_until<=clock_timestamp()));

-- name: InsertExtractionInputInventory :exec
INSERT INTO extraction_attempt_inventories(tenant_id,attempt_id,inventory_version,request_digest,fact_count,memory_count) VALUES($1,$2,1,$3,$4,$5);

-- name: InsertExtractionFactInput :exec
INSERT INTO extraction_attempt_fact_inputs(tenant_id,attempt_id,record_id,version_id,payload_retained) VALUES($1,$2,$3,$4,$5);

-- name: InsertExtractionMemoryInput :exec
INSERT INTO extraction_attempt_memory_inputs(tenant_id,attempt_id,record_id,version_id,payload_retained) VALUES($1,$2,$3,$4,$5);

-- name: HasUninventoriedExtractionAttempts :one
SELECT EXISTS(SELECT 1 FROM extraction_attempts a LEFT JOIN extraction_attempt_inventories i ON i.tenant_id=a.tenant_id AND i.attempt_id=a.id WHERE a.tenant_id=$1 AND i.attempt_id IS NULL);

-- name: ListUninventoriedExtractionAttemptPage :many
SELECT a.id,a.job_id,a.scope_id,coalesce(d.payload,p.payload)::bytea AS payload
FROM extraction_attempts a
LEFT JOIN extraction_attempt_payloads p ON p.tenant_id=a.tenant_id AND p.attempt_id=a.id
LEFT JOIN extraction_attempt_disposals d ON d.tenant_id=a.tenant_id AND d.attempt_id=a.id
LEFT JOIN extraction_attempt_inventories i ON i.tenant_id=a.tenant_id AND i.attempt_id=a.id
WHERE a.tenant_id=$1 AND i.attempt_id IS NULL
ORDER BY a.id COLLATE "C" LIMIT $2;

-- name: HasPartialExtractionInputInventory :one
SELECT EXISTS(SELECT 1 FROM extraction_attempt_fact_inputs f WHERE f.tenant_id=sqlc.arg(tenant_id) AND f.attempt_id=sqlc.arg(attempt_id))
 OR EXISTS(SELECT 1 FROM extraction_attempt_memory_inputs m WHERE m.tenant_id=sqlc.arg(tenant_id) AND m.attempt_id=sqlc.arg(attempt_id));

-- name: InsertExtractionJobPayload :exec
INSERT INTO extraction_job_payloads(tenant_id,job_id,payload) VALUES($1,$2,$3);

-- name: InsertExtractionAttemptPayload :exec
INSERT INTO extraction_attempt_payloads(tenant_id,attempt_id,payload) VALUES($1,$2,$3);

-- name: InsertExtractionResultPayload :exec
INSERT INTO extraction_result_payloads(tenant_id,attempt_id,payload) VALUES($1,$2,$3);
