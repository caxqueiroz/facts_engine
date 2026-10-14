-- +goose Up
CREATE TABLE extraction_quota(tenant_id text PRIMARY KEY,day date NOT NULL,reserved_tokens bigint NOT NULL CHECK(reserved_tokens>=0),epoch bigint NOT NULL DEFAULT 0);
CREATE TABLE extraction_attempts(tenant_id text NOT NULL,id text NOT NULL,job_id text NOT NULL,scope_id text NOT NULL,fence bigint NOT NULL,ledger_revision bigint NOT NULL,payload bytea NOT NULL,PRIMARY KEY(tenant_id,id),UNIQUE(tenant_id,job_id,fence),FOREIGN KEY(tenant_id,job_id) REFERENCES extraction_jobs(tenant_id,id),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
CREATE TABLE extraction_results(tenant_id text NOT NULL,attempt_id text NOT NULL,job_id text NOT NULL,scope_id text NOT NULL,ledger_revision bigint NOT NULL,payload bytea NOT NULL,PRIMARY KEY(tenant_id,attempt_id),FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_attempts(tenant_id,id),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
CREATE TABLE extraction_proposals(tenant_id text NOT NULL,attempt_id text NOT NULL,ordinal integer NOT NULL,record_id text NOT NULL,version_id text NOT NULL,PRIMARY KEY(tenant_id,attempt_id,ordinal),FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_results(tenant_id,attempt_id),FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES record_versions(tenant_id,record_id,id));
CREATE INDEX extraction_attempt_scope ON extraction_attempts(tenant_id,scope_id,ledger_revision);
CREATE INDEX extraction_result_scope ON extraction_results(tenant_id,scope_id,ledger_revision);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['extraction_quota','extraction_attempts','extraction_results','extraction_proposals'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
DROP TABLE extraction_proposals,extraction_results,extraction_attempts,extraction_quota;
