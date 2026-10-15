-- name: LockCalculationGuard :exec
INSERT INTO calculation_guards(tenant_id,calculation_id,epoch) VALUES($1,$2,1)
ON CONFLICT(tenant_id,calculation_id) DO UPDATE SET epoch=calculation_guards.epoch+1;

-- name: CalculationContentAvailable :one
SELECT EXISTS(SELECT 1 FROM calculation_payloads p WHERE p.tenant_id=$1 AND p.calculation_id=$2)
AND NOT EXISTS(SELECT 1 FROM calculation_disposals d WHERE d.tenant_id=$1 AND d.calculation_id=$2);
