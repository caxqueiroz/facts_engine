-- +goose Up
CREATE TABLE extraction_redactions(
 tenant_id text NOT NULL,job_id text NOT NULL,scope_id text NOT NULL,evidence_id text NOT NULL,
 PRIMARY KEY(tenant_id,job_id,evidence_id),
 FOREIGN KEY(tenant_id,job_id,scope_id) REFERENCES extraction_jobs(tenant_id,id,scope_id),
 FOREIGN KEY(tenant_id,scope_id,evidence_id) REFERENCES evidence(tenant_id,scope_id,id),
 FOREIGN KEY(tenant_id,evidence_id) REFERENCES evidence_derivations(tenant_id,evidence_id)
);
CREATE INDEX extraction_redactions_evidence ON extraction_redactions(tenant_id,evidence_id);
ALTER TABLE extraction_redactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_redactions FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_redactions USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'extraction provenance is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
