CREATE TABLE facts_projection_tombstones(id text PRIMARY KEY);
CREATE FUNCTION facts_guard_disposed_projection() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.id,0));
 IF EXISTS(SELECT 1 FROM public.facts_projection_tombstones WHERE id=NEW.id) THEN
  RAISE EXCEPTION 'projection content was disposed' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER facts_guard_disposed_projection BEFORE INSERT OR UPDATE ON embeddings
FOR EACH ROW EXECUTE FUNCTION facts_guard_disposed_projection();
