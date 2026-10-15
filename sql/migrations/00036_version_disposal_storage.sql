-- +goose Up
CREATE TABLE fact_version_guards(tenant_id text NOT NULL,version_id text NOT NULL,epoch bigint NOT NULL,
 PRIMARY KEY(tenant_id,version_id),FOREIGN KEY(tenant_id,version_id) REFERENCES record_versions(tenant_id,id));
CREATE TABLE fact_version_disposals(tenant_id text NOT NULL,version_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,version_id),FOREIGN KEY(tenant_id,version_id) REFERENCES record_versions(tenant_id,id));
ALTER TABLE fact_version_guards ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_version_guards FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON fact_version_guards USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
ALTER TABLE fact_version_disposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_version_disposals FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON fact_version_disposals USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose StatementBegin
CREATE FUNCTION guard_fact_version_content() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO fact_version_guards(tenant_id,version_id,epoch) VALUES(NEW.tenant_id,NEW.version_id,1)
 ON CONFLICT(tenant_id,version_id) DO UPDATE SET epoch=fact_version_guards.epoch+1;
 IF TG_TABLE_NAME='fact_version_payloads' AND EXISTS(SELECT 1 FROM fact_version_disposals WHERE tenant_id=NEW.tenant_id AND version_id=NEW.version_id) THEN
 RAISE EXCEPTION 'version content was disposed' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_fact_version_payload BEFORE INSERT OR UPDATE ON fact_version_payloads FOR EACH ROW EXECUTE FUNCTION guard_fact_version_content();
CREATE TRIGGER guard_fact_version_disposal BEFORE INSERT ON fact_version_disposals FOR EACH ROW EXECUTE FUNCTION guard_fact_version_content();
CREATE TABLE memory_version_guards(tenant_id text NOT NULL,version_id text NOT NULL,epoch bigint NOT NULL,
 PRIMARY KEY(tenant_id,version_id),FOREIGN KEY(tenant_id,version_id) REFERENCES memory_versions(tenant_id,id));
CREATE TABLE memory_version_disposals(tenant_id text NOT NULL,version_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,version_id),FOREIGN KEY(tenant_id,version_id) REFERENCES memory_versions(tenant_id,id));
ALTER TABLE memory_version_guards ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_version_guards FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON memory_version_guards USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
ALTER TABLE memory_version_disposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_version_disposals FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON memory_version_disposals USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose StatementBegin
CREATE FUNCTION guard_memory_version_content() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO memory_version_guards(tenant_id,version_id,epoch) VALUES(NEW.tenant_id,NEW.version_id,1)
 ON CONFLICT(tenant_id,version_id) DO UPDATE SET epoch=memory_version_guards.epoch+1;
 IF TG_TABLE_NAME='memory_version_payloads' AND EXISTS(SELECT 1 FROM memory_version_disposals WHERE tenant_id=NEW.tenant_id AND version_id=NEW.version_id) THEN
 RAISE EXCEPTION 'version content was disposed' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_memory_version_payload BEFORE INSERT OR UPDATE ON memory_version_payloads FOR EACH ROW EXECUTE FUNCTION guard_memory_version_content();
CREATE TRIGGER guard_memory_version_disposal BEFORE INSERT ON memory_version_disposals FOR EACH ROW EXECUTE FUNCTION guard_memory_version_content();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'version disposal storage is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
