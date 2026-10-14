-- +goose Up
-- NOT VALID preserves pre-registry history while enforcing every new reference.
-- Adopt existing IDs through PutScope/PutSubject, then run scripts/validate-identities.sql.
ALTER TABLE ledger_events ADD CONSTRAINT ledger_events_registered_scope FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id) NOT VALID;
ALTER TABLE records ADD CONSTRAINT records_registered_scope FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id) NOT VALID;
ALTER TABLE policy_versions ADD CONSTRAINT policy_versions_registered_scope FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id) NOT VALID;
ALTER TABLE evidence ADD CONSTRAINT evidence_registered_scope FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id) NOT VALID;
ALTER TABLE conversations ADD CONSTRAINT conversations_registered_scope FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id) NOT VALID;
ALTER TABLE extraction_jobs ADD CONSTRAINT extraction_jobs_registered_scope FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id) NOT VALID;
ALTER TABLE retention_scopes ADD CONSTRAINT retention_scopes_registered_scope FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id) NOT VALID;
ALTER TABLE context_scopes ADD CONSTRAINT context_scopes_registered_scope FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id) NOT VALID;
ALTER TABLE decision_scopes ADD CONSTRAINT decision_scopes_registered_scope FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id) NOT VALID;
ALTER TABLE fact_snapshot_items ADD CONSTRAINT fact_snapshot_items_registered_scope FOREIGN KEY(tenant_id,scope_id) REFERENCES scopes(tenant_id,id) NOT VALID;
-- Memory records have no fact subject. Keep their natural key representation intact.
ALTER TABLE records ADD COLUMN registered_subject_id text GENERATED ALWAYS AS (CASE WHEN kind='fact' THEN subject_id END) STORED;
ALTER TABLE records ADD CONSTRAINT records_registered_subject FOREIGN KEY(tenant_id,scope_id,registered_subject_id) REFERENCES subjects(tenant_id,scope_id,id) NOT VALID;
CREATE TABLE extraction_subjects(
 tenant_id text NOT NULL,job_id text NOT NULL,scope_id text NOT NULL,subject_id text NOT NULL,
 PRIMARY KEY(tenant_id,job_id,subject_id),
 FOREIGN KEY(tenant_id,job_id,scope_id) REFERENCES extraction_jobs(tenant_id,id,scope_id),
 FOREIGN KEY(tenant_id,scope_id,subject_id) REFERENCES subjects(tenant_id,scope_id,id)
);
ALTER TABLE extraction_subjects ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_subjects FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_subjects USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'identity enforcement is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
