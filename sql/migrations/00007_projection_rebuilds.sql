-- +goose Up
ALTER TABLE consumer_offsets DROP CONSTRAINT consumer_offsets_state_check;
ALTER TABLE consumer_offsets ADD CONSTRAINT consumer_offsets_state_check CHECK(state IN ('pending','running','failed','paused'));
CREATE TABLE projection_generations (
 tenant_id text NOT NULL, generation text NOT NULL,
 state text NOT NULL CHECK(state IN ('building','paused','verified','resuming','activating','active','retired','abandoned')),
 fence bigint NOT NULL CHECK(fence>0),
 through_revision bigint NOT NULL DEFAULT 0 CHECK(through_revision>=0),
 base_epoch bigint NOT NULL DEFAULT 0 CHECK(base_epoch>=0),
 previous_generation text NOT NULL DEFAULT '',
 entry_count integer NOT NULL DEFAULT 0 CHECK(entry_count>=0),
 digest text NOT NULL DEFAULT '',
 PRIMARY KEY(tenant_id,generation)
);
CREATE TABLE active_projections (
 tenant_id text PRIMARY KEY,
 generation text NOT NULL,
 epoch bigint NOT NULL CHECK(epoch>0),
 FOREIGN KEY(tenant_id,generation) REFERENCES projection_generations(tenant_id,generation)
);
CREATE TABLE projection_rebuild_events (
 tenant_id text NOT NULL,generation text NOT NULL,fence bigint NOT NULL,operation text NOT NULL,
 payload bytea NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,generation,fence,operation),
 FOREIGN KEY(tenant_id,generation) REFERENCES projection_generations(tenant_id,generation)
);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['projection_generations','active_projections','projection_rebuild_events'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING (tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose Down
-- Operational fencing cannot safely be downgraded while processes are running.
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'forward-only migration; restore a verified backup for downgrade'; END $$;
-- +goose StatementEnd
