-- +goose Up
CREATE TABLE decision_guards (
 tenant_id text NOT NULL,decision_id text NOT NULL,epoch bigint NOT NULL,
 PRIMARY KEY(tenant_id,decision_id),
 FOREIGN KEY(tenant_id,decision_id) REFERENCES decision_receipts(tenant_id,id)
);
CREATE TABLE decision_disposals (
 tenant_id text NOT NULL,decision_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,decision_id),
 FOREIGN KEY(tenant_id,decision_id) REFERENCES decision_receipts(tenant_id,id)
);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['decision_guards','decision_disposals'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING(tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK(tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE FUNCTION guard_decision_content() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.decision_id IS NULL THEN RETURN NEW; END IF;
 INSERT INTO decision_guards(tenant_id,decision_id,epoch) VALUES(NEW.tenant_id,NEW.decision_id,1)
 ON CONFLICT(tenant_id,decision_id) DO UPDATE SET epoch=decision_guards.epoch+1;
 IF TG_TABLE_NAME <> 'decision_disposals' THEN
  IF EXISTS(SELECT 1 FROM decision_disposals WHERE tenant_id=NEW.tenant_id AND decision_id=NEW.decision_id)
   OR (TG_TABLE_NAME<>'decision_payloads' AND NOT EXISTS(SELECT 1 FROM decision_payloads WHERE tenant_id=NEW.tenant_id AND decision_id=NEW.decision_id)) THEN
   RAISE EXCEPTION 'decision content is unavailable' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_decision_payload BEFORE INSERT OR UPDATE ON decision_payloads FOR EACH ROW EXECUTE FUNCTION guard_decision_content();
CREATE TRIGGER guard_decision_disposal BEFORE INSERT ON decision_disposals FOR EACH ROW EXECUTE FUNCTION guard_decision_content();
CREATE TRIGGER guard_decision_scopes_owner BEFORE INSERT OR UPDATE OF tenant_id,decision_id ON decision_scopes FOR EACH ROW EXECUTE FUNCTION guard_decision_content();
CREATE TRIGGER guard_decision_items_owner BEFORE INSERT OR UPDATE OF tenant_id,decision_id ON decision_items FOR EACH ROW EXECUTE FUNCTION guard_decision_content();
CREATE TRIGGER guard_decision_memory_items_owner BEFORE INSERT OR UPDATE OF tenant_id,decision_id ON decision_memory_items FOR EACH ROW EXECUTE FUNCTION guard_decision_content();
CREATE TRIGGER guard_decision_artifacts_owner BEFORE INSERT OR UPDATE OF tenant_id,decision_id ON decision_artifacts FOR EACH ROW EXECUTE FUNCTION guard_decision_content();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'decision disposal storage is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
