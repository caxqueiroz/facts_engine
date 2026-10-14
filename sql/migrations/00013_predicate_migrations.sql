-- +goose Up
CREATE TABLE predicate_migrations (
 tenant_id text NOT NULL, scope_id text NOT NULL, predicate text NOT NULL,
 version text NOT NULL, from_version text NOT NULL,
 mode text NOT NULL CHECK(mode='preserve_history_repropose'),
 PRIMARY KEY(tenant_id,scope_id,predicate,version),
 CHECK(version<>from_version),
 FOREIGN KEY(tenant_id,scope_id,predicate,version) REFERENCES predicate_versions(tenant_id,scope_id,predicate,version),
 FOREIGN KEY(tenant_id,scope_id,predicate,from_version) REFERENCES predicate_versions(tenant_id,scope_id,predicate,version)
);
ALTER TABLE predicate_migrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE predicate_migrations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON predicate_migrations USING (tenant_id=current_setting('app.tenant_id',true)) WITH CHECK (tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
DROP TABLE predicate_migrations;
