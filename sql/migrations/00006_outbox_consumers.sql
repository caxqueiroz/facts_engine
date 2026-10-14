-- +goose Up
ALTER TABLE ledger_events ADD CONSTRAINT ledger_event_revision_identity UNIQUE(tenant_id,id,ledger_revision);
ALTER TABLE outbox ADD CONSTRAINT outbox_revision_identity FOREIGN KEY(tenant_id,event_id,ledger_revision) REFERENCES ledger_events(tenant_id,id,ledger_revision);
CREATE UNIQUE INDEX outbox_revision ON outbox(tenant_id,ledger_revision);
CREATE TABLE consumer_offsets (
 tenant_id text NOT NULL,
 consumer_id text NOT NULL,
 acknowledged_revision bigint NOT NULL DEFAULT 0 CHECK(acknowledged_revision>=0),
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','running','failed')),
 fence bigint NOT NULL DEFAULT 0 CHECK(fence>=0),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
 max_attempts integer NOT NULL CHECK(max_attempts BETWEEN 1 AND 100),
 lease_until timestamptz,
 available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 last_failure_code text NOT NULL DEFAULT '',
 PRIMARY KEY(tenant_id,consumer_id),
 CHECK((state='running') = (lease_until IS NOT NULL))
);
ALTER TABLE consumer_offsets ENABLE ROW LEVEL SECURITY;
ALTER TABLE consumer_offsets FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON consumer_offsets USING (tenant_id=current_setting('app.tenant_id',true)) WITH CHECK (tenant_id=current_setting('app.tenant_id',true));
CREATE INDEX consumer_pending ON consumer_offsets(tenant_id,available_at) WHERE state='pending';
CREATE TABLE consumer_retry_commands (
 tenant_id text NOT NULL,
 consumer_id text NOT NULL,
 failed_fence bigint NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,consumer_id,failed_fence),
 FOREIGN KEY(tenant_id,consumer_id) REFERENCES consumer_offsets(tenant_id,consumer_id)
);
ALTER TABLE consumer_retry_commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE consumer_retry_commands FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON consumer_retry_commands USING (tenant_id=current_setting('app.tenant_id',true)) WITH CHECK (tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
DROP TABLE consumer_retry_commands,consumer_offsets;
DROP INDEX outbox_revision;
ALTER TABLE outbox DROP CONSTRAINT outbox_revision_identity;
ALTER TABLE ledger_events DROP CONSTRAINT ledger_event_revision_identity;
