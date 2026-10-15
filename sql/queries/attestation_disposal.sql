-- name: LockAttestationGuard :exec
INSERT INTO attestation_guards(tenant_id,attestation_id,epoch) VALUES($1,$2,1)
ON CONFLICT(tenant_id,attestation_id) DO UPDATE SET epoch=attestation_guards.epoch+1;
