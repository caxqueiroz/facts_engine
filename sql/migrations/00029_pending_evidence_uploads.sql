-- +goose Up
CREATE TABLE pending_evidence_uploads(
 tenant_id text NOT NULL,locator text NOT NULL,object_version text NOT NULL,
 scope_id text NOT NULL,policy_id text NOT NULL,byte_size bigint NOT NULL CHECK(byte_size>=0 AND byte_size<=16777216),
 uploaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,locator,object_version),
 FOREIGN KEY(tenant_id,locator,object_version) REFERENCES retention_objects(tenant_id,locator,object_version),
 FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id),
 FOREIGN KEY(tenant_id,scope_id,policy_id) REFERENCES retention_policies(tenant_id,scope_id,id)
);
CREATE INDEX pending_evidence_uploads_scope_age ON pending_evidence_uploads(tenant_id,scope_id,uploaded_at,locator,object_version);
ALTER TABLE pending_evidence_uploads ENABLE ROW LEVEL SECURITY;
ALTER TABLE pending_evidence_uploads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON pending_evidence_uploads USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'pending evidence upload provenance is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
