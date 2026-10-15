-- +goose Up
CREATE TABLE extraction_content_guards(tenant_id text NOT NULL,job_id text NOT NULL,epoch bigint NOT NULL,
 PRIMARY KEY(tenant_id,job_id),FOREIGN KEY(tenant_id,job_id) REFERENCES extraction_jobs(tenant_id,id));
CREATE TABLE extraction_job_disposals(tenant_id text NOT NULL,job_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(tenant_id,job_id),
 FOREIGN KEY(tenant_id,job_id) REFERENCES extraction_jobs(tenant_id,id));
CREATE TABLE extraction_attempt_disposals(tenant_id text NOT NULL,attempt_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(tenant_id,attempt_id),
 FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_attempts(tenant_id,id));
CREATE TABLE extraction_result_disposals(tenant_id text NOT NULL,attempt_id text NOT NULL,payload bytea NOT NULL,
 disposed_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(tenant_id,attempt_id),
 FOREIGN KEY(tenant_id,attempt_id) REFERENCES extraction_results(tenant_id,attempt_id));
ALTER TABLE extraction_content_guards ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_content_guards FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_content_guards USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
ALTER TABLE extraction_job_disposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_job_disposals FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_job_disposals USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
ALTER TABLE extraction_attempt_disposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_attempt_disposals FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_attempt_disposals USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
ALTER TABLE extraction_result_disposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE extraction_result_disposals FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON extraction_result_disposals USING(tenant_id=current_setting('app.tenant_id',true)) WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose StatementBegin
CREATE FUNCTION guard_extraction_content() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE job text;
BEGIN
 IF TG_ARGV[0]='job' THEN job:=NEW.job_id;
 ELSE SELECT job_id INTO job FROM extraction_attempts WHERE tenant_id=NEW.tenant_id AND id=NEW.attempt_id;
 END IF;
 INSERT INTO extraction_content_guards(tenant_id,job_id,epoch) VALUES(NEW.tenant_id,job,1)
 ON CONFLICT(tenant_id,job_id) DO UPDATE SET epoch=extraction_content_guards.epoch+1;
 IF TG_ARGV[1]='payload' THEN
  IF EXISTS(SELECT 1 FROM extraction_job_disposals WHERE tenant_id=NEW.tenant_id AND job_id=job) THEN
   RAISE EXCEPTION 'extraction job content was disposed' USING ERRCODE='23514';
  END IF;
  IF TG_ARGV[0]<>'job' THEN
   IF EXISTS(SELECT 1 FROM extraction_attempt_disposals WHERE tenant_id=NEW.tenant_id AND attempt_id=NEW.attempt_id) THEN
    RAISE EXCEPTION 'extraction attempt content was disposed' USING ERRCODE='23514';
   END IF;
   IF TG_ARGV[0]='result' AND EXISTS(SELECT 1 FROM extraction_result_disposals WHERE tenant_id=NEW.tenant_id AND attempt_id=NEW.attempt_id) THEN
    RAISE EXCEPTION 'extraction result content was disposed' USING ERRCODE='23514';
   END IF;
  END IF;
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_extraction_job_payloads BEFORE INSERT OR UPDATE ON extraction_job_payloads FOR EACH ROW EXECUTE FUNCTION guard_extraction_content('job','payload');
CREATE TRIGGER guard_extraction_job_disposals BEFORE INSERT ON extraction_job_disposals FOR EACH ROW EXECUTE FUNCTION guard_extraction_content('job','disposal');
CREATE TRIGGER guard_extraction_attempt_payloads BEFORE INSERT OR UPDATE ON extraction_attempt_payloads FOR EACH ROW EXECUTE FUNCTION guard_extraction_content('attempt','payload');
CREATE TRIGGER guard_extraction_attempt_disposals BEFORE INSERT ON extraction_attempt_disposals FOR EACH ROW EXECUTE FUNCTION guard_extraction_content('attempt','disposal');
CREATE TRIGGER guard_extraction_result_payloads BEFORE INSERT OR UPDATE ON extraction_result_payloads FOR EACH ROW EXECUTE FUNCTION guard_extraction_content('result','payload');
CREATE TRIGGER guard_extraction_result_disposals BEFORE INSERT ON extraction_result_disposals FOR EACH ROW EXECUTE FUNCTION guard_extraction_content('result','disposal');
-- +goose StatementBegin
CREATE FUNCTION guard_extraction_execution() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE job text;
BEGIN
 IF TG_TABLE_NAME='extraction_job_state' THEN job:=NEW.id; ELSE job:=NEW.job_id; END IF;
 INSERT INTO extraction_content_guards(tenant_id,job_id,epoch) VALUES(NEW.tenant_id,job,1)
 ON CONFLICT(tenant_id,job_id) DO UPDATE SET epoch=extraction_content_guards.epoch+1;
 IF EXISTS(SELECT 1 FROM extraction_job_disposals WHERE tenant_id=NEW.tenant_id AND job_id=job)
 OR NOT EXISTS(SELECT 1 FROM extraction_job_payloads WHERE tenant_id=NEW.tenant_id AND job_id=job) THEN
  RAISE EXCEPTION 'extraction job content is unavailable' USING ERRCODE='23514';
 END IF;
 IF TG_TABLE_NAME='extraction_results' THEN
  IF EXISTS(SELECT 1 FROM extraction_attempt_disposals WHERE tenant_id=NEW.tenant_id AND attempt_id=NEW.attempt_id)
  OR NOT EXISTS(SELECT 1 FROM extraction_attempt_payloads WHERE tenant_id=NEW.tenant_id AND attempt_id=NEW.attempt_id) THEN
   RAISE EXCEPTION 'extraction attempt content is unavailable' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER guard_extraction_attempt_execution BEFORE INSERT ON extraction_attempts FOR EACH ROW EXECUTE FUNCTION guard_extraction_execution();
CREATE TRIGGER guard_extraction_result_execution BEFORE INSERT ON extraction_results FOR EACH ROW EXECUTE FUNCTION guard_extraction_execution();
CREATE TRIGGER guard_extraction_state_execution BEFORE INSERT OR UPDATE OF state ON extraction_job_state FOR EACH ROW WHEN (NEW.state IN ('pending','running')) EXECUTE FUNCTION guard_extraction_execution();
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'extraction disposal storage is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
