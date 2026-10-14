-- +goose Up
CREATE TABLE extraction_refresh_triggers(
 tenant_id text NOT NULL,job_id text NOT NULL,event_id text NOT NULL,
 PRIMARY KEY(tenant_id,job_id),
 FOREIGN KEY(tenant_id,job_id) REFERENCES extraction_jobs(tenant_id,id),
 FOREIGN KEY(tenant_id,event_id) REFERENCES ledger_events(tenant_id,id)
);
CREATE INDEX refresh_trigger_jobs ON extraction_refresh_triggers(tenant_id,event_id,job_id);
ALTER TABLE extraction_refresh_triggers ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_refresh_triggers FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_refresh_triggers USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'refresh trigger provenance is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
