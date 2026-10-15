-- +goose Up
-- Payload-bearing references must serialize with source disposal. Structural
-- timelines/events remain append-only history and are deliberately not guarded.
-- +goose StatementBegin
CREATE FUNCTION guard_live_version_reference() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target text;
BEGIN
 target := to_jsonb(NEW)->>TG_ARGV[1];
 IF target IS NULL THEN RETURN NEW; END IF;
 IF TG_ARGV[0]='fact' THEN
  INSERT INTO fact_version_guards(tenant_id,version_id,epoch) VALUES(NEW.tenant_id,target,1)
  ON CONFLICT(tenant_id,version_id) DO UPDATE SET epoch=fact_version_guards.epoch+1;
  IF EXISTS(SELECT 1 FROM fact_version_disposals WHERE tenant_id=NEW.tenant_id AND version_id=target)
   OR NOT EXISTS(SELECT 1 FROM fact_version_payloads WHERE tenant_id=NEW.tenant_id AND version_id=target) THEN
   RAISE EXCEPTION 'fact version content is unavailable' USING ERRCODE='23514';
  END IF;
 ELSIF TG_ARGV[0]='memory' THEN
  INSERT INTO memory_version_guards(tenant_id,version_id,epoch) VALUES(NEW.tenant_id,target,1)
  ON CONFLICT(tenant_id,version_id) DO UPDATE SET epoch=memory_version_guards.epoch+1;
  IF EXISTS(SELECT 1 FROM memory_version_disposals WHERE tenant_id=NEW.tenant_id AND version_id=target)
   OR NOT EXISTS(SELECT 1 FROM memory_version_payloads WHERE tenant_id=NEW.tenant_id AND version_id=target) THEN
   RAISE EXCEPTION 'memory version content is unavailable' USING ERRCODE='23514';
  END IF;
 ELSE
  RAISE EXCEPTION 'unknown version reference kind' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_fact_snapshot_items_version BEFORE INSERT OR UPDATE OF tenant_id,version_id ON fact_snapshot_items FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('fact','version_id');
CREATE TRIGGER guard_context_items_version BEFORE INSERT OR UPDATE OF tenant_id,version_id ON context_items FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('fact','version_id');
CREATE TRIGGER guard_decision_items_version BEFORE INSERT OR UPDATE OF tenant_id,version_id ON decision_items FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('fact','version_id');
CREATE TRIGGER guard_attestations_version BEFORE INSERT OR UPDATE OF tenant_id,version_id ON attestations FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('fact','version_id');
CREATE TRIGGER guard_memory_dependencies_version BEFORE INSERT OR UPDATE OF tenant_id,version_id ON memory_dependencies FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('fact','version_id');
CREATE TRIGGER guard_extraction_memory_dependencies_version BEFORE INSERT OR UPDATE OF tenant_id,version_id ON extraction_memory_dependencies FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('fact','version_id');
CREATE TRIGGER guard_context_memory_items_version BEFORE INSERT OR UPDATE OF tenant_id,version_id ON context_memory_items FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('memory','version_id');
CREATE TRIGGER guard_decision_memory_items_version BEFORE INSERT OR UPDATE OF tenant_id,version_id ON decision_memory_items FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('memory','version_id');
CREATE TRIGGER guard_memory_attestations_version BEFORE INSERT OR UPDATE OF tenant_id,version_id ON memory_attestations FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('memory','version_id');
CREATE TRIGGER guard_memory_dependency_owner BEFORE INSERT OR UPDATE OF tenant_id,memory_version_id ON memory_dependencies FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('memory','memory_version_id');
CREATE TRIGGER guard_refresh_source_version BEFORE INSERT OR UPDATE OF tenant_id,memory_version_id ON extraction_jobs FOR EACH ROW EXECUTE FUNCTION guard_live_version_reference('memory','memory_version_id');
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'version reference guards are forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
