-- +goose Up
-- Stop writers and upgrade all readers/workers together. Preserve ciphertext intact.
CREATE TABLE fact_attestation_payloads(
 tenant_id text NOT NULL,attestation_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,attestation_id),
 FOREIGN KEY(tenant_id,attestation_id) REFERENCES attestations(tenant_id,id)
);
ALTER TABLE attestations NO FORCE ROW LEVEL SECURITY;
INSERT INTO fact_attestation_payloads(tenant_id,attestation_id,payload) SELECT tenant_id,id,payload FROM attestations;
ALTER TABLE attestations FORCE ROW LEVEL SECURITY;
ALTER TABLE attestations DROP COLUMN payload;
ALTER TABLE fact_attestation_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_attestation_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON fact_attestation_payloads
USING(tenant_id=current_setting('app.tenant_id',true))
WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
CREATE TABLE memory_attestation_payloads(
 tenant_id text NOT NULL,attestation_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,attestation_id),
 FOREIGN KEY(tenant_id,attestation_id) REFERENCES memory_attestations(tenant_id,id)
);
ALTER TABLE memory_attestations NO FORCE ROW LEVEL SECURITY;
INSERT INTO memory_attestation_payloads(tenant_id,attestation_id,payload) SELECT tenant_id,id,payload FROM memory_attestations;
ALTER TABLE memory_attestations FORCE ROW LEVEL SECURITY;
ALTER TABLE memory_attestations DROP COLUMN payload;
ALTER TABLE memory_attestation_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_attestation_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON memory_attestation_payloads
USING(tenant_id=current_setting('app.tenant_id',true))
WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'attestation payload separation is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
