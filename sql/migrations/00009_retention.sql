-- +goose Up
CREATE TABLE retention_domain(id boolean PRIMARY KEY DEFAULT true CHECK(id), identity uuid NOT NULL DEFAULT gen_random_uuid());
INSERT INTO retention_domain DEFAULT VALUES;
CREATE TABLE retention_scopes(tenant_id text NOT NULL,scope_id text NOT NULL,epoch bigint NOT NULL DEFAULT 0,PRIMARY KEY(tenant_id,scope_id));
CREATE TABLE retention_objects(tenant_id text NOT NULL,locator text NOT NULL,object_version text NOT NULL,epoch bigint NOT NULL DEFAULT 0,disposed boolean NOT NULL DEFAULT false,PRIMARY KEY(tenant_id,locator,object_version));
CREATE TABLE evidence_retention(tenant_id text NOT NULL,evidence_id text NOT NULL,scope_id text NOT NULL,locator text NOT NULL,object_version text NOT NULL,registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(tenant_id,evidence_id),FOREIGN KEY(tenant_id,evidence_id) REFERENCES evidence(tenant_id,id),FOREIGN KEY(tenant_id,locator,object_version) REFERENCES retention_objects(tenant_id,locator,object_version));
CREATE INDEX evidence_retention_object ON evidence_retention(tenant_id,locator,object_version);
CREATE TABLE retention_events(tenant_id text NOT NULL,id text NOT NULL,scope_id text NOT NULL,kind text NOT NULL,target_id text NOT NULL,ledger_revision bigint NOT NULL,payload bytea NOT NULL,PRIMARY KEY(tenant_id,id),FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision));
CREATE INDEX retention_scope_events ON retention_events(tenant_id,scope_id,ledger_revision);
CREATE TABLE retention_policies(tenant_id text NOT NULL,scope_id text NOT NULL,id text NOT NULL,payload bytea NOT NULL,PRIMARY KEY(tenant_id,scope_id,id));
CREATE TABLE legal_holds(tenant_id text NOT NULL,scope_id text NOT NULL,id text NOT NULL,PRIMARY KEY(tenant_id,scope_id,id));
CREATE TABLE legal_hold_releases(tenant_id text NOT NULL,scope_id text NOT NULL,id text NOT NULL,PRIMARY KEY(tenant_id,scope_id,id),FOREIGN KEY(tenant_id,scope_id,id) REFERENCES legal_holds(tenant_id,scope_id,id));
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['retention_scopes','retention_objects','evidence_retention','retention_events','retention_policies','legal_holds','legal_hold_releases'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
DROP TABLE legal_hold_releases,legal_holds,retention_policies,retention_events,evidence_retention,retention_objects,retention_scopes,retention_domain;
