-- +goose Up
CREATE TABLE attestation_identities(tenant_id text NOT NULL,id text NOT NULL,scope_id text NOT NULL,kind text NOT NULL CHECK(kind IN ('fact','memory')),PRIMARY KEY(tenant_id,id),UNIQUE(tenant_id,id,scope_id,kind));
ALTER TABLE attestations NO FORCE ROW LEVEL SECURITY;
INSERT INTO attestation_identities SELECT tenant_id,id,scope_id,'fact' FROM attestations;
ALTER TABLE attestations FORCE ROW LEVEL SECURITY;
ALTER TABLE attestations ADD COLUMN kind text NOT NULL DEFAULT 'fact' CHECK(kind='fact');
ALTER TABLE attestations ADD FOREIGN KEY(tenant_id,id,scope_id,kind) REFERENCES attestation_identities(tenant_id,id,scope_id,kind);
CREATE TABLE memory_attestations(tenant_id text NOT NULL,id text NOT NULL,scope_id text NOT NULL,record_id text NOT NULL,version_id text NOT NULL,evidence_id text NOT NULL,ledger_revision bigint NOT NULL,payload bytea NOT NULL,kind text NOT NULL DEFAULT 'memory' CHECK(kind='memory'),PRIMARY KEY(tenant_id,id),FOREIGN KEY(tenant_id,id,scope_id,kind) REFERENCES attestation_identities(tenant_id,id,scope_id,kind),FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES memory_versions(tenant_id,record_id,id),FOREIGN KEY(tenant_id,scope_id,version_id) REFERENCES memory_versions(tenant_id,scope_id,id),FOREIGN KEY(tenant_id,scope_id,evidence_id) REFERENCES evidence(tenant_id,scope_id,id),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
CREATE TABLE memory_attestation_events(tenant_id text NOT NULL,attestation_id text NOT NULL,actor text NOT NULL,ledger_revision bigint NOT NULL,PRIMARY KEY(tenant_id,attestation_id),FOREIGN KEY(tenant_id,attestation_id) REFERENCES memory_attestations(tenant_id,id),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
CREATE INDEX memory_attestations_version ON memory_attestations(tenant_id,version_id,ledger_revision);
CREATE INDEX memory_attestations_evidence ON memory_attestations(tenant_id,evidence_id);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['attestation_identities','memory_attestations','memory_attestation_events'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'memory attestations are forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
