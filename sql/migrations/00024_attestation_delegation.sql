-- +goose Up
CREATE TABLE attestation_delegations(
 tenant_id text NOT NULL,authority text NOT NULL,binding_id text NOT NULL,request_digest text NOT NULL,
 attestation_id text NOT NULL,scope_id text NOT NULL,kind text NOT NULL,
 PRIMARY KEY(tenant_id,authority,binding_id),UNIQUE(tenant_id,attestation_id),
 FOREIGN KEY(tenant_id,attestation_id,scope_id,kind) REFERENCES attestation_identities(tenant_id,id,scope_id,kind)
);
ALTER TABLE attestation_delegations ENABLE ROW LEVEL SECURITY;
ALTER TABLE attestation_delegations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON attestation_delegations USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'identity verification receipts are forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
