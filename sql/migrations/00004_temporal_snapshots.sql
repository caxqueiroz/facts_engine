-- +goose Up
-- Generations are immutable resolved timelines, ordered by the tenant ledger.
CREATE TABLE timeline_generations (
 tenant_id text NOT NULL, record_id text NOT NULL, ledger_revision bigint NOT NULL,
 PRIMARY KEY(tenant_id,record_id,ledger_revision),
 FOREIGN KEY(tenant_id,record_id) REFERENCES records(tenant_id,id),
 FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision)
);
CREATE TABLE timeline_history (
 tenant_id text NOT NULL, record_id text NOT NULL, ledger_revision bigint NOT NULL,
 ordinal integer NOT NULL, version_id text NOT NULL, payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,record_id,ledger_revision,ordinal),
 FOREIGN KEY(tenant_id,record_id,ledger_revision) REFERENCES timeline_generations(tenant_id,record_id,ledger_revision),
 FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES record_versions(tenant_id,record_id,id)
);
-- Backfill every pre-existing acceptance generation using the retained encrypted rows.
-- The migration owner temporarily bypasses FORCE RLS inside this deployment transaction.
-- Ordinary non-owner runtime sessions still remain subject to RLS.
ALTER TABLE accepted_intervals NO FORCE ROW LEVEL SECURITY;
INSERT INTO timeline_generations SELECT DISTINCT tenant_id,record_id,ledger_revision FROM accepted_intervals;
INSERT INTO timeline_history
 SELECT g.tenant_id,g.record_id,g.ledger_revision,
 (row_number() OVER(PARTITION BY g.tenant_id,g.record_id,g.ledger_revision ORDER BY a.valid_from,a.version_id))::integer,
 a.version_id,a.payload FROM timeline_generations g JOIN accepted_intervals a
 ON a.tenant_id=g.tenant_id AND a.record_id=g.record_id AND a.ledger_revision<=g.ledger_revision;
ALTER TABLE accepted_intervals FORCE ROW LEVEL SECURITY;
ALTER TABLE accepted_intervals DROP CONSTRAINT accepted_intervals_pkey;
ALTER TABLE accepted_intervals ADD COLUMN segment_id bigint GENERATED ALWAYS AS IDENTITY;
ALTER TABLE accepted_intervals ADD PRIMARY KEY(tenant_id,record_id,segment_id);
CREATE TABLE rejected_versions (
 tenant_id text NOT NULL, record_id text NOT NULL, version_id text NOT NULL, ledger_revision bigint NOT NULL,
 PRIMARY KEY(tenant_id,record_id,version_id),
 FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES record_versions(tenant_id,record_id,id),
 FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision)
);
ALTER TABLE attestations ADD COLUMN ledger_revision bigint;
ALTER TABLE attestations ADD FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision);
ALTER TABLE attestation_events ADD COLUMN ledger_revision bigint;
ALTER TABLE attestation_events ADD FOREIGN KEY(tenant_id,ledger_revision) REFERENCES ledger_events(tenant_id,ledger_revision);
-- Old attestation revisions cannot be inferred from encrypted payloads in SQL.
-- NULL rows remain usable for current reads; historical eligibility fails explicitly.
CREATE TABLE fact_snapshots (
 tenant_id text NOT NULL, id text NOT NULL, selection_revision bigint NOT NULL,
 creation_revision bigint NOT NULL, digest text NOT NULL, payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,id),
 FOREIGN KEY(tenant_id,creation_revision) REFERENCES ledger_events(tenant_id,ledger_revision)
);
CREATE TABLE fact_snapshot_items (
 tenant_id text NOT NULL, snapshot_id text NOT NULL, ordinal integer NOT NULL,
 scope_id text NOT NULL, selector_digest text NOT NULL, record_id text, version_id text,
 PRIMARY KEY(tenant_id,snapshot_id,ordinal), UNIQUE(tenant_id,snapshot_id,selector_digest),
 FOREIGN KEY(tenant_id,snapshot_id) REFERENCES fact_snapshots(tenant_id,id),
 FOREIGN KEY(tenant_id,record_id) REFERENCES records(tenant_id,id),
 FOREIGN KEY(tenant_id,record_id,version_id) REFERENCES record_versions(tenant_id,record_id,id)
);
CREATE INDEX snapshot_versions ON fact_snapshot_items(tenant_id,version_id);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['timeline_generations','timeline_history','rejected_versions','fact_snapshots','fact_snapshot_items'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING(tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK(tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
-- Forward-only: partial supersessions cannot be represented by the previous schema.
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'Temporal migration is forward-only; restore a verified backup to downgrade'; END $$;
-- +goose StatementEnd
