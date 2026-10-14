-- +goose Up
CREATE TABLE extraction_jobs(tenant_id text NOT NULL,id text NOT NULL,scope_id text NOT NULL,conversation_id text NOT NULL,profile_id text NOT NULL,profile_digest text NOT NULL,ledger_revision bigint NOT NULL,payload bytea NOT NULL,PRIMARY KEY(tenant_id,id),FOREIGN KEY(tenant_id,conversation_id) REFERENCES conversations(tenant_id,id),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
CREATE INDEX extraction_jobs_scope ON extraction_jobs(tenant_id,scope_id,ledger_revision);
CREATE TABLE extraction_job_state(tenant_id text NOT NULL,id text NOT NULL,state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','running','completed','failed')),fence bigint NOT NULL DEFAULT 0 CHECK(fence>=0),attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),max_attempts integer NOT NULL CHECK(max_attempts BETWEEN 1 AND 10),attempt_id text NOT NULL DEFAULT '',available_at timestamptz NOT NULL DEFAULT clock_timestamp(),lease_until timestamptz,last_failure_code text NOT NULL DEFAULT '',PRIMARY KEY(tenant_id,id),FOREIGN KEY(tenant_id,id) REFERENCES extraction_jobs(tenant_id,id));
CREATE INDEX extraction_pending ON extraction_job_state(tenant_id,state,available_at);
CREATE TABLE extraction_retry_events(tenant_id text NOT NULL,id text NOT NULL,job_id text NOT NULL,ledger_revision bigint NOT NULL,payload bytea NOT NULL,PRIMARY KEY(tenant_id,id),FOREIGN KEY(tenant_id,job_id) REFERENCES extraction_jobs(tenant_id,id),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['extraction_jobs','extraction_job_state','extraction_retry_events'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
DROP TABLE extraction_retry_events,extraction_job_state,extraction_jobs;
