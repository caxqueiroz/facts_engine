-- +goose Up
CREATE TABLE projection_retention_guards(tenant_id text PRIMARY KEY,epoch bigint NOT NULL);
CREATE TABLE projection_destinations(
 tenant_id text NOT NULL,generation text NOT NULL,consumer_id text NOT NULL,
 registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,generation),UNIQUE(tenant_id,consumer_id),
 CHECK(generation ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'),
 CHECK(consumer_id='projection-'||encode(sha256(convert_to('"'||generation||'"','UTF8')),'hex')),
 FOREIGN KEY(tenant_id,consumer_id) REFERENCES consumer_offsets(tenant_id,consumer_id));
-- Known managed destinations can be recovered exactly. Static consumer hashes
-- cannot be reversed and must be explicitly bound by the host after upgrade.
ALTER TABLE projection_generations NO FORCE ROW LEVEL SECURITY;
INSERT INTO projection_destinations(tenant_id,generation,consumer_id)
SELECT tenant_id,generation,'projection-'||encode(sha256(convert_to('"'||generation||'"','UTF8')),'hex') FROM projection_generations;
ALTER TABLE projection_generations FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_retention_guards ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_retention_guards FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON projection_retention_guards USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
ALTER TABLE projection_destinations ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_destinations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON projection_destinations USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose StatementBegin
CREATE FUNCTION guard_projection_retention() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO projection_retention_guards(tenant_id,epoch) VALUES(NEW.tenant_id,1)
 ON CONFLICT(tenant_id) DO UPDATE SET epoch=projection_retention_guards.epoch+1;
 IF TG_TABLE_NAME='projection_generations' AND TG_OP='INSERT' THEN
  INSERT INTO projection_destinations(tenant_id,generation,consumer_id)
  VALUES(NEW.tenant_id,NEW.generation,'projection-'||encode(sha256(convert_to('"'||NEW.generation||'"','UTF8')),'hex'))
  ON CONFLICT(tenant_id,generation) DO NOTHING;
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_projection_destination_insert BEFORE INSERT ON projection_destinations FOR EACH ROW EXECUTE FUNCTION guard_projection_retention();
CREATE TRIGGER guard_projection_generation_retention BEFORE INSERT OR UPDATE ON projection_generations FOR EACH ROW EXECUTE FUNCTION guard_projection_retention();
CREATE TRIGGER guard_projection_consumer_retention BEFORE INSERT ON consumer_offsets FOR EACH ROW WHEN (NEW.consumer_id LIKE 'projection-%') EXECUTE FUNCTION guard_projection_retention();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'projection destination inventory is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
