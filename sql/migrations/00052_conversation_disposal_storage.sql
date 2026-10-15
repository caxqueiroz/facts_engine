-- +goose Up
CREATE TABLE conversation_payloads(
 tenant_id text NOT NULL,conversation_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,conversation_id),
 FOREIGN KEY(tenant_id,conversation_id) REFERENCES conversations(tenant_id,id)
);
INSERT INTO conversation_payloads(tenant_id,conversation_id,payload)
SELECT tenant_id,id,payload FROM conversations;
ALTER TABLE conversations DROP COLUMN payload;

CREATE TABLE conversation_disposals(
 tenant_id text NOT NULL,conversation_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,conversation_id),
 FOREIGN KEY(tenant_id,conversation_id) REFERENCES conversations(tenant_id,id)
);
CREATE TABLE conversation_guards(
 tenant_id text NOT NULL,conversation_id text NOT NULL,epoch bigint NOT NULL,
 PRIMARY KEY(tenant_id,conversation_id),
 FOREIGN KEY(tenant_id,conversation_id) REFERENCES conversations(tenant_id,id)
);
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['conversation_payloads','conversation_disposals','conversation_guards'] LOOP
  EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
  EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
  EXECUTE format('CREATE POLICY tenant_boundary ON %I USING(tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK(tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE FUNCTION guard_conversation_content() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO conversation_guards(tenant_id,conversation_id,epoch) VALUES(NEW.tenant_id,NEW.conversation_id,1)
 ON CONFLICT(tenant_id,conversation_id) DO UPDATE SET epoch=conversation_guards.epoch+1;
 IF TG_TABLE_NAME='conversation_payloads' AND EXISTS(
  SELECT 1 FROM conversation_disposals WHERE tenant_id=NEW.tenant_id AND conversation_id=NEW.conversation_id
 ) THEN RAISE EXCEPTION 'conversation content was disposed' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE FUNCTION guard_live_conversation_reference() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.conversation_id IS NULL THEN RETURN NEW; END IF;
 INSERT INTO conversation_guards(tenant_id,conversation_id,epoch) VALUES(NEW.tenant_id,NEW.conversation_id,1)
 ON CONFLICT(tenant_id,conversation_id) DO UPDATE SET epoch=conversation_guards.epoch+1;
 IF EXISTS(SELECT 1 FROM conversation_disposals WHERE tenant_id=NEW.tenant_id AND conversation_id=NEW.conversation_id) THEN
  RAISE EXCEPTION 'conversation content was disposed' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_conversation_payload BEFORE INSERT OR UPDATE ON conversation_payloads FOR EACH ROW EXECUTE FUNCTION guard_conversation_content();
CREATE TRIGGER guard_conversation_disposal BEFORE INSERT ON conversation_disposals FOR EACH ROW EXECUTE FUNCTION guard_conversation_content();
CREATE TRIGGER guard_extraction_conversation BEFORE INSERT ON extraction_jobs FOR EACH ROW EXECUTE FUNCTION guard_live_conversation_reference();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'conversation disposal storage is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
