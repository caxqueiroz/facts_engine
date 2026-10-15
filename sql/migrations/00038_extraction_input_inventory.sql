-- +goose Up
CREATE TABLE extraction_attempt_inventories(
 tenant_id text NOT NULL,attempt_id text NOT NULL,inventory_version integer NOT NULL CHECK(inventory_version=1),
 request_digest text NOT NULL,fact_count integer NOT NULL CHECK(fact_count>=0),memory_count integer NOT NULL CHECK(memory_count>=0),
 PRIMARY KEY(tenant_id,attempt_id),FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_attempts(tenant_id,id));
CREATE TABLE extraction_attempt_fact_inputs(
 tenant_id text NOT NULL,attempt_id text NOT NULL,record_id text NOT NULL,version_id text NOT NULL,payload_retained boolean NOT NULL,
 PRIMARY KEY(tenant_id,attempt_id,version_id),FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_attempts(tenant_id,id),
 FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES record_versions(tenant_id,record_id,id));
CREATE INDEX extraction_attempt_fact_inputs_version ON extraction_attempt_fact_inputs(tenant_id,version_id,attempt_id);
CREATE TRIGGER guard_extraction_attempt_fact_inputs BEFORE INSERT OR UPDATE OF tenant_id,version_id,payload_retained ON extraction_attempt_fact_inputs FOR EACH ROW WHEN (NEW.payload_retained) EXECUTE FUNCTION guard_live_version_reference('fact','version_id');
CREATE TABLE extraction_attempt_memory_inputs(
 tenant_id text NOT NULL,attempt_id text NOT NULL,record_id text NOT NULL,version_id text NOT NULL,payload_retained boolean NOT NULL,
 PRIMARY KEY(tenant_id,attempt_id,version_id),FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_attempts(tenant_id,id),
 FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES memory_versions(tenant_id,record_id,id));
CREATE INDEX extraction_attempt_memory_inputs_version ON extraction_attempt_memory_inputs(tenant_id,version_id,attempt_id);
CREATE TRIGGER guard_extraction_attempt_memory_inputs BEFORE INSERT OR UPDATE OF tenant_id,version_id,payload_retained ON extraction_attempt_memory_inputs FOR EACH ROW WHEN (NEW.payload_retained) EXECUTE FUNCTION guard_live_version_reference('memory','version_id');
ALTER TABLE extraction_attempt_inventories ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_attempt_inventories FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_attempt_inventories USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
ALTER TABLE extraction_attempt_fact_inputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_attempt_fact_inputs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_attempt_fact_inputs USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
ALTER TABLE extraction_attempt_memory_inputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_attempt_memory_inputs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_attempt_memory_inputs USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'extraction input inventory is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
