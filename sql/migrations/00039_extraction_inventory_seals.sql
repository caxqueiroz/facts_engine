-- +goose Up
CREATE TABLE extraction_inventory_guards(tenant_id text NOT NULL,attempt_id text NOT NULL,epoch bigint NOT NULL,
 PRIMARY KEY(tenant_id,attempt_id),FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_attempts(tenant_id,id));
ALTER TABLE extraction_inventory_guards ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_inventory_guards FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_inventory_guards USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- Validate existing receipts as the migration owner, across every tenant.
ALTER TABLE extraction_attempt_inventories NO FORCE ROW LEVEL SECURITY;
ALTER TABLE extraction_attempt_fact_inputs NO FORCE ROW LEVEL SECURITY;
ALTER TABLE extraction_attempt_memory_inputs NO FORCE ROW LEVEL SECURITY;
-- +goose StatementBegin
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM extraction_attempt_inventories i
 WHERE i.fact_count<>(SELECT count(*) FROM extraction_attempt_fact_inputs f WHERE f.tenant_id=i.tenant_id AND f.attempt_id=i.attempt_id)
 OR i.memory_count<>(SELECT count(*) FROM extraction_attempt_memory_inputs m WHERE m.tenant_id=i.tenant_id AND m.attempt_id=i.attempt_id)) THEN
 RAISE EXCEPTION 'existing extraction inventory counts do not match retained references' USING ERRCODE='23514';
 END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE extraction_attempt_inventories FORCE ROW LEVEL SECURITY;
ALTER TABLE extraction_attempt_fact_inputs FORCE ROW LEVEL SECURITY;
ALTER TABLE extraction_attempt_memory_inputs FORCE ROW LEVEL SECURITY;
-- +goose StatementBegin
CREATE FUNCTION guard_extraction_inventory_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO extraction_inventory_guards(tenant_id,attempt_id,epoch) VALUES(NEW.tenant_id,NEW.attempt_id,1)
 ON CONFLICT(tenant_id,attempt_id) DO UPDATE SET epoch=extraction_inventory_guards.epoch+1;
 IF TG_TABLE_NAME='extraction_attempt_inventories' THEN
  IF NEW.fact_count<>(SELECT count(*) FROM extraction_attempt_fact_inputs WHERE tenant_id=NEW.tenant_id AND attempt_id=NEW.attempt_id)
   OR NEW.memory_count<>(SELECT count(*) FROM extraction_attempt_memory_inputs WHERE tenant_id=NEW.tenant_id AND attempt_id=NEW.attempt_id) THEN
   RAISE EXCEPTION 'extraction inventory counts do not match retained references' USING ERRCODE='23514';
  END IF;
 ELSE
  IF EXISTS(SELECT 1 FROM extraction_attempt_inventories WHERE tenant_id=NEW.tenant_id AND attempt_id=NEW.attempt_id) THEN
   RAISE EXCEPTION 'extraction inventory is already complete' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_inventory_fact_insert BEFORE INSERT ON extraction_attempt_fact_inputs FOR EACH ROW EXECUTE FUNCTION guard_extraction_inventory_completion();
CREATE TRIGGER guard_inventory_memory_insert BEFORE INSERT ON extraction_attempt_memory_inputs FOR EACH ROW EXECUTE FUNCTION guard_extraction_inventory_completion();
CREATE TRIGGER guard_inventory_completion BEFORE INSERT ON extraction_attempt_inventories FOR EACH ROW EXECUTE FUNCTION guard_extraction_inventory_completion();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'extraction inventory sealing is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
