-- +goose Up
CREATE TABLE active_predicates(tenant_id text NOT NULL,scope_id text NOT NULL,predicate text NOT NULL,version text NOT NULL,PRIMARY KEY(tenant_id,scope_id,predicate),FOREIGN KEY(tenant_id,scope_id,predicate,version) REFERENCES predicate_versions(tenant_id,scope_id,predicate,version));
ALTER TABLE active_predicates ENABLE ROW LEVEL SECURITY;
ALTER TABLE active_predicates FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON active_predicates USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
DROP TABLE active_predicates;
