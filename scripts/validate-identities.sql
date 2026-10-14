-- Run as migration owner after governed adoption of every inventory entry.
-- ON_ERROR_STOP prevents partial success from being mistaken for completion.
\set ON_ERROR_STOP on
BEGIN;
ALTER TABLE ledger_events VALIDATE CONSTRAINT ledger_events_registered_scope;
ALTER TABLE records VALIDATE CONSTRAINT records_registered_scope;
ALTER TABLE policy_versions VALIDATE CONSTRAINT policy_versions_registered_scope;
ALTER TABLE evidence VALIDATE CONSTRAINT evidence_registered_scope;
ALTER TABLE conversations VALIDATE CONSTRAINT conversations_registered_scope;
ALTER TABLE extraction_jobs VALIDATE CONSTRAINT extraction_jobs_registered_scope;
ALTER TABLE retention_scopes VALIDATE CONSTRAINT retention_scopes_registered_scope;
ALTER TABLE context_scopes VALIDATE CONSTRAINT context_scopes_registered_scope;
ALTER TABLE decision_scopes VALIDATE CONSTRAINT decision_scopes_registered_scope;
ALTER TABLE fact_snapshot_items VALIDATE CONSTRAINT fact_snapshot_items_registered_scope;
ALTER TABLE records VALIDATE CONSTRAINT records_registered_subject;
COMMIT;
