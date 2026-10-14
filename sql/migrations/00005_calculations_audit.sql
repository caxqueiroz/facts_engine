-- +goose Up
CREATE TABLE calculation_receipts (
 tenant_id text NOT NULL, id text NOT NULL, scope_id text NOT NULL,
 snapshot_id text NOT NULL, application_id text NOT NULL, external_run_id text NOT NULL,
 request_digest text NOT NULL, ledger_revision bigint NOT NULL, payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,application_id,external_run_id),
 FOREIGN KEY(tenant_id,snapshot_id) REFERENCES fact_snapshots(tenant_id,id),
 FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision)
);
CREATE TABLE calculation_snapshots (
 tenant_id text NOT NULL, calculation_id text NOT NULL, snapshot_id text NOT NULL,
 PRIMARY KEY(tenant_id,calculation_id,snapshot_id),
 FOREIGN KEY(tenant_id,calculation_id) REFERENCES calculation_receipts(tenant_id,id),
 FOREIGN KEY(tenant_id,snapshot_id) REFERENCES fact_snapshots(tenant_id,id)
);
CREATE TABLE calculation_artifacts (
 tenant_id text NOT NULL, calculation_id text NOT NULL, evidence_id text NOT NULL, role text NOT NULL,
 PRIMARY KEY(tenant_id,calculation_id,role),
 FOREIGN KEY(tenant_id,calculation_id) REFERENCES calculation_receipts(tenant_id,id),
 FOREIGN KEY(tenant_id,evidence_id) REFERENCES evidence(tenant_id,id)
);
CREATE INDEX calculations_snapshot ON calculation_snapshots(tenant_id,snapshot_id);
CREATE TABLE audit_export_requests (
 tenant_id text NOT NULL, id text NOT NULL, scope_id text NOT NULL,
 ledger_revision bigint NOT NULL, payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,id), FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision)
);
CREATE TABLE audit_export_jobs (
 tenant_id text NOT NULL, id text NOT NULL, state text NOT NULL DEFAULT 'pending',
 fence bigint NOT NULL DEFAULT 0, lease_until timestamptz,
 PRIMARY KEY(tenant_id,id), FOREIGN KEY(tenant_id,id) REFERENCES audit_export_requests(tenant_id,id),
 CHECK(state IN ('pending','running','completed'))
);
CREATE INDEX pending_audit_exports ON audit_export_jobs(tenant_id,state,lease_until);
CREATE TABLE audit_exports (
 tenant_id text NOT NULL, id text NOT NULL, ledger_revision bigint NOT NULL,
 digest text NOT NULL, payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,id), FOREIGN KEY(tenant_id,id) REFERENCES audit_export_requests(tenant_id,id),
 FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision)
);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['calculation_receipts','calculation_snapshots','calculation_artifacts','audit_export_requests','audit_export_jobs','audit_exports'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING(tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK(tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
DROP TABLE audit_exports,audit_export_jobs,audit_export_requests,calculation_artifacts,calculation_snapshots,calculation_receipts;
