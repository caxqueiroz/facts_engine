-- +goose Up
-- Stop writers and upgrade readers/workers together. Move ciphertext intact.
CREATE TABLE fact_version_payloads(
 tenant_id text NOT NULL,version_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,version_id),
 FOREIGN KEY(tenant_id,version_id) REFERENCES record_versions(tenant_id,id)
);
ALTER TABLE record_versions NO FORCE ROW LEVEL SECURITY;
INSERT INTO fact_version_payloads(tenant_id,version_id,payload) SELECT tenant_id,id,payload FROM record_versions;
ALTER TABLE record_versions FORCE ROW LEVEL SECURITY;
ALTER TABLE record_versions DROP COLUMN payload;
ALTER TABLE fact_version_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_version_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON fact_version_payloads
USING(tenant_id=current_setting('app.tenant_id',true))
WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
CREATE TABLE memory_version_payloads(
 tenant_id text NOT NULL,version_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,version_id),
 FOREIGN KEY(tenant_id,version_id) REFERENCES memory_versions(tenant_id,id)
);
ALTER TABLE memory_versions NO FORCE ROW LEVEL SECURITY;
INSERT INTO memory_version_payloads(tenant_id,version_id,payload) SELECT tenant_id,id,payload FROM memory_versions;
ALTER TABLE memory_versions FORCE ROW LEVEL SECURITY;
ALTER TABLE memory_versions DROP COLUMN payload;
ALTER TABLE memory_version_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_version_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON memory_version_payloads
USING(tenant_id=current_setting('app.tenant_id',true))
WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'version payload separation is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
