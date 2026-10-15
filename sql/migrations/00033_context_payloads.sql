-- +goose Up
-- Stop writers and upgrade readers/workers together. Ciphertext is moved intact.
CREATE TABLE context_payloads(
 tenant_id text NOT NULL,context_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,context_id),
 FOREIGN KEY(tenant_id,context_id) REFERENCES context_manifests(tenant_id,id)
);
ALTER TABLE context_manifests NO FORCE ROW LEVEL SECURITY;
INSERT INTO context_payloads(tenant_id,context_id,payload)
SELECT tenant_id,id,payload FROM context_manifests;
ALTER TABLE context_manifests FORCE ROW LEVEL SECURITY;
ALTER TABLE context_manifests DROP COLUMN payload;
ALTER TABLE context_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE context_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON context_payloads
USING(tenant_id=current_setting('app.tenant_id',true))
WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'context payload separation is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
