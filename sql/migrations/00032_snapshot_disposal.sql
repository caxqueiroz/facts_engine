-- +goose Up
CREATE TABLE snapshot_guards(tenant_id text NOT NULL,snapshot_id text NOT NULL,epoch bigint NOT NULL,
 PRIMARY KEY(tenant_id,snapshot_id),FOREIGN KEY(tenant_id,snapshot_id) REFERENCES fact_snapshots(tenant_id,id));
CREATE TABLE snapshot_disposals(tenant_id text NOT NULL,snapshot_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,snapshot_id),FOREIGN KEY(tenant_id,snapshot_id) REFERENCES fact_snapshots(tenant_id,id));
-- +goose StatementBegin
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['snapshot_guards','snapshot_disposals'] LOOP
 EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
 EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',t);
 EXECUTE format('CREATE POLICY tenant_boundary ON %I USING(tenant_id=current_setting(''app.tenant_id'',true)) WITH CHECK(tenant_id=current_setting(''app.tenant_id'',true))',t);
 END LOOP;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE FUNCTION guard_snapshot_payload_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO snapshot_guards(tenant_id,snapshot_id,epoch) VALUES(NEW.tenant_id,NEW.snapshot_id,1)
 ON CONFLICT(tenant_id,snapshot_id) DO UPDATE SET epoch=snapshot_guards.epoch+1;
 IF EXISTS(SELECT 1 FROM snapshot_disposals WHERE tenant_id=NEW.tenant_id AND snapshot_id=NEW.snapshot_id) THEN
 RAISE EXCEPTION 'snapshot content was disposed' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_snapshot_payload_insert BEFORE INSERT ON fact_snapshot_payloads FOR EACH ROW EXECUTE FUNCTION guard_snapshot_payload_insert();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'snapshot disposal is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
