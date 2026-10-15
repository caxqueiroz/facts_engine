ALTER TABLE facts_projection_gate ADD COLUMN disposal_fence bigint;
ALTER TABLE facts_projection_gate ADD CONSTRAINT facts_disposal_gate_state CHECK (
 disposal_fence IS NULL OR (disposal_fence >= 1 AND disposal_fence < 9223372036854775807 AND sealed AND fence = disposal_fence + 1)
);
CREATE FUNCTION facts_guard_disposal_gate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.disposal_fence IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
  RAISE EXCEPTION 'disposal generation cannot be reopened or refenced' USING ERRCODE='23514';
 END IF;
 IF OLD.disposal_fence IS NULL AND NEW.disposal_fence IS NOT NULL AND
    (NOT OLD.sealed OR NEW.disposal_fence <> OLD.fence OR NEW.fence <> OLD.fence + 1 OR NOT NEW.sealed) THEN
  RAISE EXCEPTION 'disposal transition requires the sealed generation fence' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER facts_guard_disposal_gate BEFORE UPDATE ON facts_projection_gate
FOR EACH ROW EXECUTE FUNCTION facts_guard_disposal_gate();
