-- +goose Up
ALTER TABLE memory_versions ADD UNIQUE(tenant_id,scope_id,record_id,id);
CREATE TABLE context_memory_items(
 tenant_id text NOT NULL,context_id text NOT NULL,ordinal integer NOT NULL CHECK(ordinal>=0),
 record_id text NOT NULL,version_id text NOT NULL,scope_id text NOT NULL,
 PRIMARY KEY(tenant_id,context_id,ordinal),UNIQUE(tenant_id,context_id,version_id),
 FOREIGN KEY(tenant_id,context_id) REFERENCES context_manifests(tenant_id,id),
 FOREIGN KEY(tenant_id,scope_id,record_id,version_id) REFERENCES memory_versions(tenant_id,scope_id,record_id,id)
);
CREATE INDEX context_memory_dependencies ON context_memory_items(tenant_id,version_id,context_id);
CREATE TABLE decision_memory_items(
 tenant_id text NOT NULL,decision_id text NOT NULL,role text NOT NULL CHECK(role IN ('selected','used')),
 ordinal integer NOT NULL CHECK(ordinal>=0),record_id text NOT NULL,version_id text NOT NULL,scope_id text NOT NULL,
 PRIMARY KEY(tenant_id,decision_id,role,ordinal),UNIQUE(tenant_id,decision_id,role,version_id),
 FOREIGN KEY(tenant_id,decision_id) REFERENCES decision_receipts(tenant_id,id),
 FOREIGN KEY(tenant_id,scope_id,record_id,version_id) REFERENCES memory_versions(tenant_id,scope_id,record_id,id)
);
CREATE INDEX decision_memory_dependencies ON decision_memory_items(tenant_id,version_id,decision_id);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['context_memory_items','decision_memory_items'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'memory contexts are forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
