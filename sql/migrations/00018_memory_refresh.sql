-- +goose Up
ALTER TABLE extraction_jobs ALTER COLUMN conversation_id DROP NOT NULL;
ALTER TABLE extraction_jobs ADD COLUMN memory_record_id text;
ALTER TABLE extraction_jobs ADD COLUMN memory_version_id text;
ALTER TABLE extraction_jobs ADD CHECK ((conversation_id IS NOT NULL AND memory_record_id IS NULL AND memory_version_id IS NULL) OR (conversation_id IS NULL AND memory_record_id IS NOT NULL AND memory_version_id IS NOT NULL));
ALTER TABLE extraction_jobs ADD FOREIGN KEY(tenant_id,scope_id,memory_record_id,memory_version_id) REFERENCES memory_versions(tenant_id,scope_id,record_id,id);
CREATE TABLE extraction_memory_proposals(tenant_id text NOT NULL,attempt_id text NOT NULL,ordinal integer NOT NULL,record_id text NOT NULL,version_id text NOT NULL,PRIMARY KEY(tenant_id,attempt_id,ordinal),FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_results(tenant_id,attempt_id),FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES memory_versions(tenant_id,record_id,id));
ALTER TABLE extraction_memory_proposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_memory_proposals FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_memory_proposals USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'memory refresh is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
