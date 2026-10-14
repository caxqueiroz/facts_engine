-- +goose Up
CREATE TABLE conversations(tenant_id text NOT NULL,id text NOT NULL,scope_id text NOT NULL,ledger_revision bigint NOT NULL,payload bytea NOT NULL,PRIMARY KEY(tenant_id,id),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
CREATE INDEX conversations_scope ON conversations(tenant_id,scope_id,ledger_revision);
CREATE TABLE conversation_messages(tenant_id text NOT NULL,conversation_id text NOT NULL,ordinal integer NOT NULL CHECK(ordinal>=0),evidence_id text NOT NULL,PRIMARY KEY(tenant_id,conversation_id,ordinal),UNIQUE(tenant_id,conversation_id,evidence_id),FOREIGN KEY(tenant_id,conversation_id) REFERENCES conversations(tenant_id,id),FOREIGN KEY(tenant_id,evidence_id) REFERENCES evidence(tenant_id,id));
CREATE INDEX conversation_evidence_dependencies ON conversation_messages(tenant_id,evidence_id);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['conversations','conversation_messages'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
DROP TABLE conversation_messages,conversations;
