-- +goose Up
CREATE TABLE scopes(
 tenant_id text NOT NULL,id text NOT NULL,current_revision bigint NOT NULL CHECK(current_revision>=0),
 PRIMARY KEY(tenant_id,id)
);
CREATE TABLE scope_versions(
 tenant_id text NOT NULL,scope_id text NOT NULL,revision bigint NOT NULL CHECK(revision>0),ledger_revision bigint NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,scope_id,revision),
 FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id),
 FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision)
);
CREATE TABLE subjects(
 tenant_id text NOT NULL,scope_id text NOT NULL,id text NOT NULL,kind text NOT NULL,current_revision bigint NOT NULL CHECK(current_revision>=0),
 PRIMARY KEY(tenant_id,scope_id,id),
 FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id)
);
CREATE TABLE subject_versions(
 tenant_id text NOT NULL,scope_id text NOT NULL,subject_id text NOT NULL,revision bigint NOT NULL CHECK(revision>0),ledger_revision bigint NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,scope_id,subject_id,revision),
 FOREIGN KEY(tenant_id,scope_id,subject_id) REFERENCES subjects(tenant_id,scope_id,id),
 FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision)
);
ALTER TABLE scopes ADD FOREIGN KEY(tenant_id,id,current_revision) REFERENCES scope_versions(tenant_id,scope_id,revision) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE subjects ADD FOREIGN KEY(tenant_id,scope_id,id,current_revision) REFERENCES subject_versions(tenant_id,scope_id,subject_id,revision) DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX scope_versions_ledger ON scope_versions(tenant_id,ledger_revision);
CREATE INDEX subject_versions_ledger ON subject_versions(tenant_id,ledger_revision);
CREATE TABLE subject_lookup(
 tenant_id text NOT NULL,scope_id text NOT NULL,namespace text NOT NULL,value_digest text NOT NULL,subject_id text NOT NULL,
 PRIMARY KEY(tenant_id,scope_id,namespace,value_digest),
 FOREIGN KEY(tenant_id,scope_id,subject_id) REFERENCES subjects(tenant_id,scope_id,id)
);
CREATE INDEX subject_lookup_identity ON subject_lookup(tenant_id,scope_id,subject_id);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['scopes','scope_versions','subjects','subject_versions','subject_lookup'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'identity history is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
