-- +goose Up
-- Do not grandfather a proof that already overlaps unacknowledged cleanup.
ALTER TABLE projection_generations NO FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_cleanup_targets NO FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_cleanup_acknowledgements NO FORCE ROW LEVEL SECURITY;
-- +goose StatementBegin
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM projection_generations g JOIN projection_cleanup_targets t USING(tenant_id,generation)
 WHERE g.state IN ('paused','verified','activating')
 AND NOT EXISTS(SELECT 1 FROM projection_cleanup_acknowledgements a WHERE a.tenant_id=t.tenant_id AND a.kind=t.kind AND a.version_id=t.version_id AND a.generation=t.generation)) THEN
  RAISE EXCEPTION 'resolve frozen generations with pending cleanup before migration' USING ERRCODE='23514';
 END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE projection_generations FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_cleanup_targets FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_cleanup_acknowledgements FORCE ROW LEVEL SECURITY;
-- +goose StatementBegin
CREATE FUNCTION guard_projection_cleanup_verification() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.state IN ('paused','verified','activating') THEN
  INSERT INTO projection_retention_guards(tenant_id,epoch) VALUES(NEW.tenant_id,1)
  ON CONFLICT(tenant_id) DO UPDATE SET epoch=projection_retention_guards.epoch+1;
  IF EXISTS(SELECT 1 FROM projection_cleanup_targets t WHERE t.tenant_id=NEW.tenant_id AND t.generation=NEW.generation
  AND NOT EXISTS(SELECT 1 FROM projection_cleanup_acknowledgements a WHERE a.tenant_id=t.tenant_id AND a.kind=t.kind AND a.version_id=t.version_id AND a.generation=t.generation)) THEN
   RAISE EXCEPTION 'projection cleanup is pending' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_projection_cleanup_verification BEFORE INSERT OR UPDATE OF state ON projection_generations FOR EACH ROW EXECUTE FUNCTION guard_projection_cleanup_verification();
-- +goose StatementBegin
CREATE FUNCTION guard_version_disposal_verification() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO projection_retention_guards(tenant_id,epoch) VALUES(NEW.tenant_id,1)
 ON CONFLICT(tenant_id) DO UPDATE SET epoch=projection_retention_guards.epoch+1;
 IF EXISTS(SELECT 1 FROM projection_generations WHERE tenant_id=NEW.tenant_id AND state IN ('paused','verified','activating')) THEN
  RAISE EXCEPTION 'frozen projection verification blocks disposal' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
-- Acquire the projection guard before the existing per-version guard triggers.
CREATE TRIGGER guard_00_fact_disposal_verification BEFORE INSERT ON fact_version_disposals FOR EACH ROW EXECUTE FUNCTION guard_version_disposal_verification();
CREATE TRIGGER guard_00_memory_disposal_verification BEFORE INSERT ON memory_version_disposals FOR EACH ROW EXECUTE FUNCTION guard_version_disposal_verification();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'projection verification fencing is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
