-- +goose Up
CREATE TABLE context_guards(tenant_id text NOT NULL,context_id text NOT NULL,epoch bigint NOT NULL,
 PRIMARY KEY(tenant_id,context_id),FOREIGN KEY(tenant_id,context_id) REFERENCES context_manifests(tenant_id,id));
CREATE TABLE context_disposals(tenant_id text NOT NULL,context_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,context_id),FOREIGN KEY(tenant_id,context_id) REFERENCES context_manifests(tenant_id,id));
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['context_guards','context_disposals'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING(tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK(tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE FUNCTION guard_context_payload_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO context_guards(tenant_id,context_id,epoch) VALUES(NEW.tenant_id,NEW.context_id,1)
 ON CONFLICT(tenant_id,context_id) DO UPDATE SET epoch=context_guards.epoch+1;
 IF EXISTS(SELECT 1 FROM context_disposals WHERE tenant_id=NEW.tenant_id AND context_id=NEW.context_id) THEN
 RAISE EXCEPTION 'context content was disposed' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_context_payload_insert BEFORE INSERT ON context_payloads FOR EACH ROW EXECUTE FUNCTION guard_context_payload_insert();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'context disposal is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
