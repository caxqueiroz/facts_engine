-- +goose Up
CREATE TABLE evidence_objects(tenant_id text NOT NULL,locator text NOT NULL,object_version text NOT NULL,payload bytea NOT NULL,PRIMARY KEY(tenant_id,locator,object_version));
ALTER TABLE evidence_objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE evidence_objects FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON evidence_objects USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
DROP TABLE evidence_objects;
