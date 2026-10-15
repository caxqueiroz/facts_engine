-- name: LockConversationGuard :exec
INSERT INTO conversation_guards(tenant_id,conversation_id,epoch) VALUES($1,$2,1)
ON CONFLICT(tenant_id,conversation_id) DO UPDATE SET epoch=conversation_guards.epoch+1;

-- name: ConversationDisposalHeader :one
SELECT c.scope_id,c.ledger_revision,e.recorded_at
FROM conversations c JOIN ledger_events e ON e.tenant_id=c.tenant_id AND e.ledger_revision=c.ledger_revision
WHERE c.tenant_id=$1 AND c.id=$2;

-- name: ConversationDisposalBlocked :one
SELECT EXISTS(SELECT 1 FROM extraction_jobs j JOIN extraction_job_payloads p ON p.tenant_id=j.tenant_id AND p.job_id=j.id WHERE j.tenant_id=sqlc.arg(tenant_id)::text AND j.conversation_id=sqlc.arg(conversation_id)::text)
OR EXISTS(SELECT 1 FROM audit_export_requests aer WHERE aer.tenant_id=sqlc.arg(tenant_id)::text AND aer.scope_id=sqlc.arg(scope_id)::text)
OR EXISTS(SELECT 1 FROM consumer_offsets co WHERE co.tenant_id=sqlc.arg(tenant_id)::text AND co.consumer_id LIKE 'archive-%' AND co.acknowledged_revision<sqlc.arg(ledger_revision)::bigint);

-- name: InsertConversationDisposal :exec
INSERT INTO conversation_disposals(tenant_id,conversation_id,payload) VALUES($1,$2,$3);

-- name: DeleteConversationPayload :execrows
DELETE FROM conversation_payloads WHERE tenant_id=$1 AND conversation_id=$2;
