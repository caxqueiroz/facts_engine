-- +goose Up
CREATE TABLE evidence_derivations(
 tenant_id text NOT NULL, scope_id text NOT NULL, evidence_id text NOT NULL, parent_evidence_id text NOT NULL,
 PRIMARY KEY(tenant_id,evidence_id), CHECK(evidence_id<>parent_evidence_id),
 FOREIGN KEY(tenant_id,scope_id,evidence_id) REFERENCES evidence(tenant_id,scope_id,id),
 FOREIGN KEY(tenant_id,scope_id,parent_evidence_id) REFERENCES evidence(tenant_id,scope_id,id)
);
CREATE INDEX evidence_derivations_parent ON evidence_derivations(tenant_id,parent_evidence_id);
ALTER TABLE evidence_derivations ENABLE ROW LEVEL SECURITY;
ALTER TABLE evidence_derivations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON evidence_derivations USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'evidence provenance is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
