-- name: LockDecisionGuard :exec
INSERT INTO decision_guards(tenant_id,decision_id,epoch) VALUES($1,$2,1)
ON CONFLICT(tenant_id,decision_id) DO UPDATE SET epoch=decision_guards.epoch+1;
