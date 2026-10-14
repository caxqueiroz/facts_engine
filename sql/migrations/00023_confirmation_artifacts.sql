-- +goose Up
CREATE TABLE confirmation_artifacts(
 tenant_id text NOT NULL,attestation_id text NOT NULL,scope_id text NOT NULL,kind text NOT NULL,
 role text NOT NULL CHECK(role IN ('presentation','response')),evidence_id text NOT NULL,digest text NOT NULL,
 object_locator text NOT NULL,object_version text NOT NULL,
 PRIMARY KEY(tenant_id,attestation_id,role),
 FOREIGN KEY(tenant_id,attestation_id,scope_id,kind) REFERENCES attestation_identities(tenant_id,id,scope_id,kind),
 FOREIGN KEY(tenant_id,scope_id,evidence_id) REFERENCES evidence(tenant_id,scope_id,id)
);
-- A response object confirms one exact version, even if registered under another evidence ID.
CREATE UNIQUE INDEX confirmation_response_once ON confirmation_artifacts(tenant_id,object_locator,object_version) WHERE role='response';
CREATE INDEX confirmation_artifacts_evidence ON confirmation_artifacts(tenant_id,evidence_id);
ALTER TABLE confirmation_artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE confirmation_artifacts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON confirmation_artifacts USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'confirmation provenance is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
