-- +goose Up
ALTER TABLE extraction_jobs ADD UNIQUE(tenant_id,id,scope_id);
CREATE TABLE extraction_memory_targets(
 tenant_id text NOT NULL,job_id text NOT NULL,id text NOT NULL,scope_id text NOT NULL,subtype text NOT NULL,schema_version text NOT NULL,
 PRIMARY KEY(tenant_id,job_id,id),
 FOREIGN KEY(tenant_id,job_id,scope_id) REFERENCES extraction_jobs(tenant_id,id,scope_id),
 FOREIGN KEY(tenant_id,scope_id,subtype,schema_version) REFERENCES memory_types(tenant_id,scope_id,subtype,version)
);
CREATE TABLE extraction_memory_dependencies(
 tenant_id text NOT NULL,job_id text NOT NULL,target_id text NOT NULL,scope_id text NOT NULL,record_id text NOT NULL,version_id text NOT NULL,
 PRIMARY KEY(tenant_id,job_id,target_id,version_id),
 FOREIGN KEY(tenant_id,job_id,target_id) REFERENCES extraction_memory_targets(tenant_id,job_id,id),
 FOREIGN KEY(tenant_id,scope_id,record_id,version_id) REFERENCES record_versions(tenant_id,scope_id,record_id,id)
);
CREATE INDEX extraction_memory_dependency_versions ON extraction_memory_dependencies(tenant_id,record_id,version_id);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['extraction_memory_targets','extraction_memory_dependencies'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'memory extraction bindings are forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
