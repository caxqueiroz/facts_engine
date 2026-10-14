-- Run explicitly as the migration owner. Set authentication outside this file.
DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='facts_runtime') THEN CREATE ROLE facts_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;
GRANT USAGE ON SCHEMA public TO facts_runtime;
GRANT SELECT, INSERT ON tenant_revisions,policy_versions,predicate_versions,evidence,records,record_versions,version_evidence,attestations,attestation_events,ledger_events,record_events,accepted_intervals,command_receipts,outbox TO facts_runtime;
GRANT UPDATE(revision) ON tenant_revisions TO facts_runtime;
GRANT UPDATE(stream_revision) ON records TO facts_runtime;
REVOKE UPDATE, DELETE, TRUNCATE ON policy_versions,predicate_versions,evidence,record_versions,version_evidence,attestations,attestation_events,ledger_events,record_events,command_receipts FROM facts_runtime;
GRANT SELECT,INSERT ON evidence_objects TO facts_runtime;
GRANT SELECT,INSERT,UPDATE ON active_predicates TO facts_runtime;
GRANT SELECT,INSERT ON timeline_generations,timeline_history,rejected_versions,fact_snapshots,fact_snapshot_items TO facts_runtime;
GRANT DELETE ON accepted_intervals TO facts_runtime;
GRANT USAGE ON SEQUENCE accepted_intervals_segment_id_seq TO facts_runtime;
GRANT SELECT,INSERT ON calculation_receipts,calculation_snapshots,calculation_artifacts,audit_export_requests,audit_export_jobs,audit_exports TO facts_runtime;
GRANT UPDATE ON audit_export_jobs TO facts_runtime;
