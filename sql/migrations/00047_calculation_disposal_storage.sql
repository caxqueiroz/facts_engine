-- +goose Up
CREATE TABLE calculation_guards (
 tenant_id text NOT NULL,calculation_id text NOT NULL,epoch bigint NOT NULL,
 PRIMARY KEY(tenant_id,calculation_id),
 FOREIGN KEY(tenant_id,calculation_id) REFERENCES calculation_receipts(tenant_id,id)
);
CREATE TABLE calculation_disposals (
 tenant_id text NOT NULL,calculation_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,calculation_id),
 FOREIGN KEY(tenant_id,calculation_id) REFERENCES calculation_receipts(tenant_id,id)
);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['calculation_guards','calculation_disposals'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING(tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK(tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE FUNCTION guard_calculation_content() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.calculation_id IS NULL THEN RETURN NEW; END IF;
 INSERT INTO calculation_guards(tenant_id,calculation_id,epoch) VALUES(NEW.tenant_id,NEW.calculation_id,1)
 ON CONFLICT(tenant_id,calculation_id) DO UPDATE SET epoch=calculation_guards.epoch+1;
 IF TG_TABLE_NAME <> 'calculation_disposals' THEN
  IF EXISTS(SELECT 1 FROM calculation_disposals WHERE tenant_id=NEW.tenant_id AND calculation_id=NEW.calculation_id)
   OR (TG_TABLE_NAME='decision_receipts' AND NOT EXISTS(SELECT 1 FROM calculation_payloads WHERE tenant_id=NEW.tenant_id AND calculation_id=NEW.calculation_id)) THEN
   RAISE EXCEPTION 'calculation content is unavailable' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_calculation_payload BEFORE INSERT OR UPDATE ON calculation_payloads FOR EACH ROW EXECUTE FUNCTION guard_calculation_content();
CREATE TRIGGER guard_calculation_disposal BEFORE INSERT ON calculation_disposals FOR EACH ROW EXECUTE FUNCTION guard_calculation_content();
CREATE TRIGGER guard_decision_calculation BEFORE INSERT OR UPDATE OF tenant_id,calculation_id ON decision_receipts FOR EACH ROW EXECUTE FUNCTION guard_calculation_content();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'calculation disposal storage is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
