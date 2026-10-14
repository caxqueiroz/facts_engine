-- +goose Up
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE TABLE tenant_revisions (tenant_id text PRIMARY KEY, revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0));
CREATE TABLE policy_versions (tenant_id text NOT NULL, scope_id text NOT NULL, id text NOT NULL, payload bytea NOT NULL, PRIMARY KEY(tenant_id,scope_id,id));
CREATE TABLE predicate_versions (tenant_id text NOT NULL, scope_id text NOT NULL, predicate text NOT NULL, version text NOT NULL, policy_id text NOT NULL, payload bytea NOT NULL, PRIMARY KEY(tenant_id,scope_id,predicate,version), FOREIGN KEY(tenant_id,scope_id,policy_id) REFERENCES policy_versions(tenant_id,scope_id,id));
CREATE TABLE evidence (tenant_id text NOT NULL, id text NOT NULL, scope_id text NOT NULL, source_identity text NOT NULL, digest text NOT NULL, payload bytea NOT NULL, PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,scope_id,source_identity), UNIQUE(tenant_id,scope_id,id));
CREATE TABLE records (tenant_id text NOT NULL, id text NOT NULL, scope_id text NOT NULL, subject_id text NOT NULL, predicate text NOT NULL, discriminator text NOT NULL, stream_revision bigint NOT NULL DEFAULT 0 CHECK(stream_revision>=0), PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,scope_id,subject_id,predicate,discriminator), UNIQUE(tenant_id,id,scope_id));
CREATE TABLE record_versions (tenant_id text NOT NULL, record_id text NOT NULL, id text NOT NULL, scope_id text NOT NULL, predicate text NOT NULL, schema_version text NOT NULL, digest text NOT NULL, payload bytea NOT NULL, PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,record_id,id), UNIQUE(tenant_id,scope_id,id), UNIQUE(tenant_id,scope_id,record_id,id), FOREIGN KEY(tenant_id,record_id,scope_id) REFERENCES records(tenant_id,id,scope_id), FOREIGN KEY(tenant_id,scope_id,predicate,schema_version) REFERENCES predicate_versions(tenant_id,scope_id,predicate,version));
CREATE TABLE version_evidence (tenant_id text NOT NULL, version_id text NOT NULL, evidence_id text NOT NULL, scope_id text NOT NULL, PRIMARY KEY(tenant_id,version_id,evidence_id), FOREIGN KEY(tenant_id,scope_id,version_id) REFERENCES record_versions(tenant_id,scope_id,id), FOREIGN KEY(tenant_id,scope_id,evidence_id) REFERENCES evidence(tenant_id,scope_id,id));
CREATE TABLE attestations (tenant_id text NOT NULL, id text NOT NULL, scope_id text NOT NULL, record_id text NOT NULL, version_id text NOT NULL, evidence_id text NOT NULL, payload bytea NOT NULL, PRIMARY KEY(tenant_id,id), FOREIGN KEY(tenant_id,scope_id,record_id,version_id) REFERENCES record_versions(tenant_id,scope_id,record_id,id), FOREIGN KEY(tenant_id,scope_id,evidence_id) REFERENCES evidence(tenant_id,scope_id,id));
CREATE TABLE attestation_events (tenant_id text NOT NULL, attestation_id text NOT NULL, actor text NOT NULL, recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(tenant_id,attestation_id), FOREIGN KEY(tenant_id,attestation_id) REFERENCES attestations(tenant_id,id));
CREATE TABLE ledger_events (tenant_id text NOT NULL, id text NOT NULL, ledger_revision bigint NOT NULL, event_type text NOT NULL, scope_id text NOT NULL, payload bytea NOT NULL, recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(tenant_id,id), UNIQUE(tenant_id,ledger_revision));
CREATE TABLE record_events (tenant_id text NOT NULL, event_id text NOT NULL, record_id text NOT NULL, version_id text NOT NULL, stream_revision bigint NOT NULL, PRIMARY KEY(tenant_id,event_id), UNIQUE(tenant_id,record_id,stream_revision), FOREIGN KEY(tenant_id,event_id) REFERENCES ledger_events(tenant_id,id), FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES record_versions(tenant_id,record_id,id));
CREATE TABLE accepted_intervals (tenant_id text NOT NULL, record_id text NOT NULL, version_id text NOT NULL, valid_from timestamptz NOT NULL, valid_to timestamptz, ledger_revision bigint, payload bytea NOT NULL, PRIMARY KEY(tenant_id,record_id,version_id), FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES record_versions(tenant_id,record_id,id), FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision), CHECK(valid_to IS NULL OR valid_to>valid_from), EXCLUDE USING gist (tenant_id WITH =,record_id WITH =,tstzrange(valid_from,valid_to,'[)') WITH &&));
CREATE TABLE command_receipts (tenant_id text NOT NULL, principal text NOT NULL, operation text NOT NULL, command_key text NOT NULL, digest text NOT NULL, payload bytea NOT NULL, PRIMARY KEY(tenant_id,principal,operation,command_key));
CREATE TABLE outbox (tenant_id text NOT NULL, event_id text NOT NULL, ledger_revision bigint NOT NULL, PRIMARY KEY(tenant_id,event_id), FOREIGN KEY(tenant_id,event_id) REFERENCES ledger_events(tenant_id,id));
CREATE INDEX evidence_scope ON evidence(tenant_id,scope_id);
CREATE INDEX attestations_version ON attestations(tenant_id,version_id);
-- +goose StatementBegin
DO $$
DECLARE t text;
BEGIN
 FOREACH t IN ARRAY ARRAY['tenant_revisions','policy_versions','predicate_versions','evidence','records','record_versions','version_evidence','attestations','attestation_events','ledger_events','record_events','accepted_intervals','command_receipts','outbox'] LOOP
  EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
  EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
  EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id = current_setting(''app.tenant_id'', true)) WITH CHECK (tenant_id = current_setting(''app.tenant_id'', true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- Runtime privileges are granted separately by scripts/runtime-role.sql.
-- +goose Down
DROP TABLE outbox,command_receipts,accepted_intervals,record_events,ledger_events,attestation_events,attestations,version_evidence,record_versions,records,evidence,predicate_versions,policy_versions,tenant_revisions;
