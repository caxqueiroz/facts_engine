-- +goose Up
CREATE TABLE attestation_guards(
 tenant_id text NOT NULL,attestation_id text NOT NULL,epoch bigint NOT NULL,
 PRIMARY KEY(tenant_id,attestation_id),
 FOREIGN KEY(tenant_id,attestation_id) REFERENCES attestation_identities(tenant_id,id)
);
ALTER TABLE attestation_guards ENABLE ROW LEVEL SECURITY;
ALTER TABLE attestation_guards FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON attestation_guards USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
CREATE TABLE fact_attestation_disposals(
 tenant_id text NOT NULL,attestation_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,attestation_id),
 FOREIGN KEY(tenant_id,attestation_id) REFERENCES attestations(tenant_id,id)
);
ALTER TABLE fact_attestation_disposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_attestation_disposals FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON fact_attestation_disposals USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
CREATE TABLE memory_attestation_disposals(
 tenant_id text NOT NULL,attestation_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,attestation_id),
 FOREIGN KEY(tenant_id,attestation_id) REFERENCES memory_attestations(tenant_id,id)
);
ALTER TABLE memory_attestation_disposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_attestation_disposals FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON memory_attestation_disposals USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose StatementBegin
CREATE FUNCTION guard_attestation_content() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 INSERT INTO attestation_guards(tenant_id,attestation_id,epoch) VALUES(NEW.tenant_id,NEW.attestation_id,1)
 ON CONFLICT(tenant_id,attestation_id) DO UPDATE SET epoch=attestation_guards.epoch+1;
 IF TG_TABLE_NAME IN ('fact_attestation_payloads','memory_attestation_payloads') AND (
 EXISTS(SELECT 1 FROM fact_attestation_disposals WHERE tenant_id=NEW.tenant_id AND attestation_id=NEW.attestation_id)
 OR EXISTS(SELECT 1 FROM memory_attestation_disposals WHERE tenant_id=NEW.tenant_id AND attestation_id=NEW.attestation_id)) THEN
 RAISE EXCEPTION 'attestation content was disposed' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_fact_attestation_payload BEFORE INSERT OR UPDATE ON fact_attestation_payloads FOR EACH ROW EXECUTE FUNCTION guard_attestation_content();
CREATE TRIGGER guard_fact_attestation_disposal BEFORE INSERT ON fact_attestation_disposals FOR EACH ROW EXECUTE FUNCTION guard_attestation_content();
CREATE TRIGGER guard_memory_attestation_payload BEFORE INSERT OR UPDATE ON memory_attestation_payloads FOR EACH ROW EXECUTE FUNCTION guard_attestation_content();
CREATE TRIGGER guard_memory_attestation_disposal BEFORE INSERT ON memory_attestation_disposals FOR EACH ROW EXECUTE FUNCTION guard_attestation_content();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'attestation disposal storage is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
