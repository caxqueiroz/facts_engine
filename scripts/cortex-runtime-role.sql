-- Run explicitly in EACH dedicated projection database after cortex.Provision.
-- Provision a separate login and grant membership; do not use the migration login.
DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='facts_cortex_runtime') THEN CREATE ROLE facts_cortex_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;
GRANT USAGE ON SCHEMA public TO facts_cortex_runtime;
GRANT SELECT ON collections,facts_projection_identity,facts_projection_tombstones TO facts_cortex_runtime;
GRANT SELECT,INSERT,UPDATE ON embeddings TO facts_cortex_runtime;
GRANT SELECT,INSERT ON facts_projection_receipts TO facts_cortex_runtime;
-- SELECT FOR SHARE requires UPDATE privileges; this login is a trusted projection
-- writer. Keep management APIs behind the host's operator authorization boundary.
REVOKE UPDATE ON facts_projection_gate FROM facts_cortex_runtime;
GRANT SELECT ON facts_projection_gate TO facts_cortex_runtime;
GRANT UPDATE(fence,sealed) ON facts_projection_gate TO facts_cortex_runtime;

-- Use a separate host-authorized retention login for destination cleanup.
DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='facts_cortex_retention') THEN CREATE ROLE facts_cortex_retention NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;
GRANT USAGE ON SCHEMA public TO facts_cortex_retention;
GRANT SELECT ON collections,facts_projection_identity,facts_projection_tombstones TO facts_cortex_retention;
GRANT SELECT,UPDATE ON facts_projection_gate TO facts_cortex_retention;
GRANT SELECT,DELETE ON embeddings,facts_projection_receipts TO facts_cortex_retention;
GRANT INSERT ON facts_projection_tombstones TO facts_cortex_retention;
