-- +goose Up
CREATE TABLE context_manifests(
 tenant_id text NOT NULL,id text NOT NULL,scope_id text NOT NULL,
 selection_revision bigint NOT NULL CHECK(selection_revision>=0),
 ledger_revision bigint NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,id),
 FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision)
);
CREATE TABLE context_scopes(
 tenant_id text NOT NULL,context_id text NOT NULL,scope_id text NOT NULL,
 PRIMARY KEY(tenant_id,context_id,scope_id),
 FOREIGN KEY(tenant_id,context_id) REFERENCES context_manifests(tenant_id,id)
);
CREATE INDEX context_scope_lookup ON context_scopes(tenant_id,scope_id,context_id);
CREATE TABLE context_items(
 tenant_id text NOT NULL,context_id text NOT NULL,ordinal integer NOT NULL CHECK(ordinal>=0),
 record_id text NOT NULL,version_id text NOT NULL,scope_id text NOT NULL,
 PRIMARY KEY(tenant_id,context_id,ordinal),UNIQUE(tenant_id,context_id,version_id),
 FOREIGN KEY(tenant_id,context_id) REFERENCES context_manifests(tenant_id,id),
 FOREIGN KEY(tenant_id,scope_id,record_id,version_id) REFERENCES record_versions(tenant_id,scope_id,record_id,id)
);
CREATE INDEX context_version_dependencies ON context_items(tenant_id,version_id,context_id);
CREATE TABLE decision_receipts(
 tenant_id text NOT NULL,id text NOT NULL,scope_id text NOT NULL,
 application_id text NOT NULL,external_decision_id text NOT NULL,request_digest text NOT NULL,
 context_id text,snapshot_id text,calculation_id text,ledger_revision bigint NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,id),UNIQUE(tenant_id,application_id,external_decision_id),
 CHECK((context_id IS NOT NULL)<>(snapshot_id IS NOT NULL)),
 FOREIGN KEY(tenant_id,context_id) REFERENCES context_manifests(tenant_id,id),
 FOREIGN KEY(tenant_id,snapshot_id) REFERENCES fact_snapshots(tenant_id,id),
 FOREIGN KEY(tenant_id,calculation_id) REFERENCES calculation_receipts(tenant_id,id),
 FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision)
);
CREATE INDEX decision_context ON decision_receipts(tenant_id,context_id);
CREATE INDEX decision_snapshot ON decision_receipts(tenant_id,snapshot_id);
CREATE INDEX decision_calculation ON decision_receipts(tenant_id,calculation_id);
CREATE TABLE decision_scopes(
 tenant_id text NOT NULL,decision_id text NOT NULL,scope_id text NOT NULL,
 PRIMARY KEY(tenant_id,decision_id,scope_id),
 FOREIGN KEY(tenant_id,decision_id) REFERENCES decision_receipts(tenant_id,id)
);
CREATE INDEX decision_scope_lookup ON decision_scopes(tenant_id,scope_id,decision_id);
CREATE TABLE decision_items(
 tenant_id text NOT NULL,decision_id text NOT NULL,role text NOT NULL CHECK(role IN ('selected','used')),
 ordinal integer NOT NULL CHECK(ordinal>=0),record_id text NOT NULL,version_id text NOT NULL,scope_id text NOT NULL,
 PRIMARY KEY(tenant_id,decision_id,role,ordinal),UNIQUE(tenant_id,decision_id,role,version_id),
 FOREIGN KEY(tenant_id,decision_id) REFERENCES decision_receipts(tenant_id,id),
 FOREIGN KEY(tenant_id,scope_id,record_id,version_id) REFERENCES record_versions(tenant_id,scope_id,record_id,id)
);
CREATE INDEX decision_version_dependencies ON decision_items(tenant_id,version_id,decision_id);
CREATE TABLE decision_artifacts(
 tenant_id text NOT NULL,decision_id text NOT NULL,role text NOT NULL,
 evidence_id text NOT NULL,
 PRIMARY KEY(tenant_id,decision_id,role),
 FOREIGN KEY(tenant_id,decision_id) REFERENCES decision_receipts(tenant_id,id),
 FOREIGN KEY(tenant_id,evidence_id) REFERENCES evidence(tenant_id,id)
);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['context_manifests','context_scopes','context_items','decision_receipts','decision_scopes','decision_items','decision_artifacts'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
DROP TABLE decision_artifacts,decision_items,decision_scopes,decision_receipts,context_items,context_scopes,context_manifests;
