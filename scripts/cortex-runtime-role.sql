-- Run explicitly in EACH dedicated projection database after cortex.Provision.
-- Provision a separate login and grant membership; do not use the migration login.
DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='facts_cortex_runtime') THEN CREATE ROLE facts_cortex_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;
GRANT USAGE ON SCHEMA public TO facts_cortex_runtime;
GRANT SELECT ON collections,facts_projection_identity TO facts_cortex_runtime;
GRANT SELECT,INSERT,UPDATE ON embeddings TO facts_cortex_runtime;
GRANT SELECT,INSERT ON facts_projection_receipts TO facts_cortex_runtime;
