-- Run explicitly as migration owner in each existing Cortex generation.
-- New cortex.Provision calls create this index automatically.
CREATE INDEX IF NOT EXISTS facts_lexical_content ON embeddings USING gin(to_tsvector('simple',content));
