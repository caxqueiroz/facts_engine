-- +goose Up
CREATE TABLE projection_cleanup_jobs(
 tenant_id text NOT NULL,kind text NOT NULL,version_id text NOT NULL,
 fact_version_id text,memory_version_id text,
 inventory_epoch bigint NOT NULL CHECK(inventory_epoch>0),
 target_count integer NOT NULL CHECK(target_count BETWEEN 0 AND 10000),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,kind,version_id),
 CHECK((kind='fact' AND fact_version_id IS NOT NULL AND fact_version_id=version_id AND memory_version_id IS NULL)
 OR (kind='memory' AND memory_version_id IS NOT NULL AND memory_version_id=version_id AND fact_version_id IS NULL)),
 FOREIGN KEY(tenant_id,fact_version_id) REFERENCES fact_version_disposals(tenant_id,version_id),
 FOREIGN KEY(tenant_id,memory_version_id) REFERENCES memory_version_disposals(tenant_id,version_id));
CREATE TABLE projection_cleanup_targets(
 tenant_id text NOT NULL,kind text NOT NULL,version_id text NOT NULL,generation text NOT NULL,
 PRIMARY KEY(tenant_id,kind,version_id,generation),
 FOREIGN KEY(tenant_id,kind,version_id) REFERENCES projection_cleanup_jobs(tenant_id,kind,version_id),
 FOREIGN KEY(tenant_id,generation) REFERENCES projection_destinations(tenant_id,generation));
CREATE TABLE projection_cleanup_acknowledgements(
 tenant_id text NOT NULL,kind text NOT NULL,version_id text NOT NULL,generation text NOT NULL,
 acknowledgement_id text NOT NULL CHECK(acknowledgement_id ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'),
 completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,kind,version_id,generation),
 FOREIGN KEY(tenant_id,kind,version_id,generation) REFERENCES projection_cleanup_targets(tenant_id,kind,version_id,generation));
-- +goose StatementBegin
CREATE FUNCTION capture_projection_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE record_kind text; captured_epoch bigint; destinations integer;
BEGIN
 IF TG_TABLE_NAME='fact_version_disposals' THEN record_kind:='fact';
 ELSIF TG_TABLE_NAME='memory_version_disposals' THEN record_kind:='memory';
 ELSE RAISE EXCEPTION 'unsupported disposal source' USING ERRCODE='23514'; END IF;
 INSERT INTO projection_retention_guards(tenant_id,epoch) VALUES(NEW.tenant_id,1)
 ON CONFLICT(tenant_id) DO UPDATE SET epoch=projection_retention_guards.epoch+1 RETURNING epoch INTO captured_epoch;
 IF EXISTS(SELECT 1 FROM consumer_offsets c WHERE c.tenant_id=NEW.tenant_id AND c.consumer_id LIKE 'projection-%'
 AND NOT EXISTS(SELECT 1 FROM projection_destinations d WHERE d.tenant_id=c.tenant_id AND d.consumer_id=c.consumer_id)) THEN
  RAISE EXCEPTION 'projection destination inventory is incomplete' USING ERRCODE='23514';
 END IF;
 SELECT count(*) INTO destinations FROM projection_destinations WHERE tenant_id=NEW.tenant_id;
 INSERT INTO projection_cleanup_jobs(tenant_id,kind,version_id,fact_version_id,memory_version_id,inventory_epoch,target_count)
 VALUES(NEW.tenant_id,record_kind,NEW.version_id,CASE WHEN record_kind='fact' THEN NEW.version_id END,
 CASE WHEN record_kind='memory' THEN NEW.version_id END,captured_epoch,destinations);
 INSERT INTO projection_cleanup_targets(tenant_id,kind,version_id,generation)
 SELECT NEW.tenant_id,record_kind,NEW.version_id,generation FROM projection_destinations WHERE tenant_id=NEW.tenant_id;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER capture_fact_projection_cleanup AFTER INSERT ON fact_version_disposals FOR EACH ROW EXECUTE FUNCTION capture_projection_cleanup();
CREATE TRIGGER capture_memory_projection_cleanup AFTER INSERT ON memory_version_disposals FOR EACH ROW EXECUTE FUNCTION capture_projection_cleanup();
-- Backfill current known destinations for earlier tombstones. Do not silently
-- declare old disposals complete while their static consumers remain unmapped.
ALTER TABLE fact_version_disposals NO FORCE ROW LEVEL SECURITY;
ALTER TABLE memory_version_disposals NO FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_destinations NO FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_retention_guards NO FORCE ROW LEVEL SECURITY;
ALTER TABLE consumer_offsets NO FORCE ROW LEVEL SECURITY;
-- +goose StatementBegin
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM consumer_offsets c WHERE c.consumer_id LIKE 'projection-%'
 AND (EXISTS(SELECT 1 FROM fact_version_disposals f WHERE f.tenant_id=c.tenant_id) OR EXISTS(SELECT 1 FROM memory_version_disposals m WHERE m.tenant_id=c.tenant_id))
 AND NOT EXISTS(SELECT 1 FROM projection_destinations d WHERE d.tenant_id=c.tenant_id AND d.consumer_id=c.consumer_id)) THEN
  RAISE EXCEPTION 'register legacy projection destinations before cleanup migration' USING ERRCODE='23514';
 END IF;
END $$;
-- +goose StatementEnd
INSERT INTO projection_retention_guards(tenant_id,epoch)
SELECT tenant_id,1 FROM (SELECT tenant_id FROM fact_version_disposals UNION SELECT tenant_id FROM memory_version_disposals) t
ON CONFLICT(tenant_id) DO UPDATE SET epoch=projection_retention_guards.epoch+1;
INSERT INTO projection_cleanup_jobs(tenant_id,kind,version_id,fact_version_id,memory_version_id,inventory_epoch,target_count)
SELECT v.tenant_id,v.kind,v.version_id,v.fact_version_id,v.memory_version_id,g.epoch,
 (SELECT count(*) FROM projection_destinations d WHERE d.tenant_id=v.tenant_id)
FROM (SELECT tenant_id,'fact' AS kind,version_id,version_id AS fact_version_id,NULL::text AS memory_version_id FROM fact_version_disposals
 UNION ALL SELECT tenant_id,'memory',version_id,NULL::text,version_id FROM memory_version_disposals) v
JOIN projection_retention_guards g ON g.tenant_id=v.tenant_id;
INSERT INTO projection_cleanup_targets(tenant_id,kind,version_id,generation)
SELECT j.tenant_id,j.kind,j.version_id,d.generation FROM projection_cleanup_jobs j JOIN projection_destinations d ON d.tenant_id=j.tenant_id;
ALTER TABLE fact_version_disposals FORCE ROW LEVEL SECURITY;
ALTER TABLE memory_version_disposals FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_destinations FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_retention_guards FORCE ROW LEVEL SECURITY;
ALTER TABLE consumer_offsets FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_cleanup_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_cleanup_jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON projection_cleanup_jobs USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
ALTER TABLE projection_cleanup_targets ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_cleanup_targets FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON projection_cleanup_targets USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
ALTER TABLE projection_cleanup_acknowledgements ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_cleanup_acknowledgements FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON projection_cleanup_acknowledgements USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'projection cleanup obligations are forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
