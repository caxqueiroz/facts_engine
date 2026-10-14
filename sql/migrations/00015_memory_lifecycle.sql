-- +goose Up
CREATE TABLE memory_timeline_generations(tenant_id text NOT NULL,record_id text NOT NULL,ledger_revision bigint NOT NULL,PRIMARY KEY(tenant_id,record_id,ledger_revision),FOREIGN KEY(tenant_id,record_id) REFERENCES records(tenant_id,id),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
CREATE TABLE memory_timeline_history(tenant_id text NOT NULL,record_id text NOT NULL,ledger_revision bigint NOT NULL,ordinal integer NOT NULL,version_id text NOT NULL,valid_from timestamptz NOT NULL,valid_to timestamptz,payload bytea NOT NULL,PRIMARY KEY(tenant_id,record_id,ledger_revision,ordinal),FOREIGN KEY(tenant_id,record_id,ledger_revision) REFERENCES memory_timeline_generations(tenant_id,record_id,ledger_revision),FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES memory_versions(tenant_id,record_id,id),CHECK(valid_to IS NULL OR valid_to>valid_from),EXCLUDE USING gist(tenant_id WITH =,record_id WITH =,ledger_revision WITH =,tstzrange(valid_from,valid_to,'[)') WITH &&));
CREATE TABLE memory_rejections(tenant_id text NOT NULL,record_id text NOT NULL,version_id text NOT NULL,ledger_revision bigint NOT NULL,PRIMARY KEY(tenant_id,record_id,version_id),FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES memory_versions(tenant_id,record_id,id),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['memory_timeline_generations','memory_timeline_history','memory_rejections'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'memory lifecycle is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
