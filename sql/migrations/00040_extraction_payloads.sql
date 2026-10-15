-- +goose Up
-- Stop writers and upgrade extraction/audit/archive readers together. Preserve ciphertext intact.
CREATE TABLE extraction_job_payloads(tenant_id text NOT NULL,job_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,job_id),FOREIGN KEY(tenant_id,job_id) REFERENCES extraction_jobs(tenant_id,id));
ALTER TABLE extraction_jobs NO FORCE ROW LEVEL SECURITY;
INSERT INTO extraction_job_payloads(tenant_id,job_id,payload) SELECT tenant_id,id,payload FROM extraction_jobs;
ALTER TABLE extraction_jobs FORCE ROW LEVEL SECURITY;
ALTER TABLE extraction_jobs DROP COLUMN payload;
ALTER TABLE extraction_job_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_job_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_job_payloads USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
CREATE TABLE extraction_attempt_payloads(tenant_id text NOT NULL,attempt_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,attempt_id),FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_attempts(tenant_id,id));
ALTER TABLE extraction_attempts NO FORCE ROW LEVEL SECURITY;
INSERT INTO extraction_attempt_payloads(tenant_id,attempt_id,payload) SELECT tenant_id,id,payload FROM extraction_attempts;
ALTER TABLE extraction_attempts FORCE ROW LEVEL SECURITY;
ALTER TABLE extraction_attempts DROP COLUMN payload;
ALTER TABLE extraction_attempt_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_attempt_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_attempt_payloads USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
CREATE TABLE extraction_result_payloads(tenant_id text NOT NULL,attempt_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,attempt_id),FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_results(tenant_id,attempt_id));
ALTER TABLE extraction_results NO FORCE ROW LEVEL SECURITY;
INSERT INTO extraction_result_payloads(tenant_id,attempt_id,payload) SELECT tenant_id,attempt_id,payload FROM extraction_results;
ALTER TABLE extraction_results FORCE ROW LEVEL SECURITY;
ALTER TABLE extraction_results DROP COLUMN payload;
ALTER TABLE extraction_result_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_result_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_result_payloads USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'extraction payload separation is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
