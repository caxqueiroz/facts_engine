-- +goose Up
ALTER TABLE records ADD COLUMN kind text NOT NULL DEFAULT 'fact' CHECK(kind IN ('fact','memory'));
ALTER TABLE records ADD UNIQUE(tenant_id,id,kind);
ALTER TABLE record_versions ADD COLUMN kind text NOT NULL DEFAULT 'fact' CHECK(kind='fact');
ALTER TABLE record_versions ADD FOREIGN KEY(tenant_id,record_id,kind) REFERENCES records(tenant_id,id,kind);
CREATE TABLE memory_types(tenant_id text NOT NULL,scope_id text NOT NULL,subtype text NOT NULL,version text NOT NULL,policy_id text NOT NULL,from_version text,ledger_revision bigint NOT NULL,payload bytea NOT NULL,PRIMARY KEY(tenant_id,scope_id,subtype,version),FOREIGN KEY(tenant_id,scope_id,policy_id) REFERENCES policy_versions(tenant_id,scope_id,id),FOREIGN KEY(tenant_id,scope_id,subtype,from_version) REFERENCES memory_types(tenant_id,scope_id,subtype,version),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
CREATE TABLE active_memory_types(tenant_id text NOT NULL,scope_id text NOT NULL,subtype text NOT NULL,version text NOT NULL,PRIMARY KEY(tenant_id,scope_id,subtype),FOREIGN KEY(tenant_id,scope_id,subtype,version) REFERENCES memory_types(tenant_id,scope_id,subtype,version) DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE memory_versions(tenant_id text NOT NULL,id text NOT NULL,record_id text NOT NULL,scope_id text NOT NULL,subtype text NOT NULL,schema_version text NOT NULL,ledger_revision bigint NOT NULL,payload bytea NOT NULL,kind text NOT NULL DEFAULT 'memory' CHECK(kind='memory'),PRIMARY KEY(tenant_id,id),UNIQUE(tenant_id,record_id,id),UNIQUE(tenant_id,scope_id,id),FOREIGN KEY(tenant_id,record_id,kind) REFERENCES records(tenant_id,id,kind),FOREIGN KEY(tenant_id,record_id,scope_id) REFERENCES records(tenant_id,id,scope_id),FOREIGN KEY(tenant_id,scope_id,subtype,schema_version) REFERENCES memory_types(tenant_id,scope_id,subtype,version),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
CREATE TABLE memory_evidence(tenant_id text NOT NULL,scope_id text NOT NULL,version_id text NOT NULL,evidence_id text NOT NULL,PRIMARY KEY(tenant_id,version_id,evidence_id),FOREIGN KEY(tenant_id,scope_id,version_id) REFERENCES memory_versions(tenant_id,scope_id,id),FOREIGN KEY(tenant_id,scope_id,evidence_id) REFERENCES evidence(tenant_id,scope_id,id));
CREATE TABLE memory_dependencies(tenant_id text NOT NULL,memory_version_id text NOT NULL,scope_id text NOT NULL,record_id text NOT NULL,version_id text NOT NULL,PRIMARY KEY(tenant_id,memory_version_id,version_id),FOREIGN KEY(tenant_id,memory_version_id) REFERENCES memory_versions(tenant_id,id),FOREIGN KEY(tenant_id,scope_id,record_id,version_id) REFERENCES record_versions(tenant_id,scope_id,record_id,id));
CREATE TABLE memory_record_events(tenant_id text NOT NULL,event_id text NOT NULL,record_id text NOT NULL,version_id text NOT NULL,stream_revision bigint NOT NULL,PRIMARY KEY(tenant_id,event_id),UNIQUE(tenant_id,record_id,stream_revision),FOREIGN KEY(tenant_id,event_id) REFERENCES ledger_events(tenant_id,id),FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES memory_versions(tenant_id,record_id,id));
CREATE INDEX memory_versions_scope_revision ON memory_versions(tenant_id,scope_id,ledger_revision);
CREATE INDEX memory_dependencies_source ON memory_dependencies(tenant_id,record_id,version_id);
CREATE INDEX memory_evidence_source ON memory_evidence(tenant_id,evidence_id);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['memory_types','active_memory_types','memory_versions','memory_evidence','memory_dependencies','memory_record_events'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
-- Common memory identities must be removed only through a reviewed restore; never silently downgrade.
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'memory proposal migration is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
