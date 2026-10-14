-- Run explicitly as the migration owner. Set authentication outside this file.
CREATE ROLE facts_runtime NOLOGIN NOSUPERUSER NOBYPASSRLS;
GRANT USAGE ON SCHEMA public TO facts_runtime;
GRANT SELECT, INSERT ON tenant_revisions,policy_versions,predicate_versions,evidence,records,record_versions,version_evidence,attestations,attestation_events,ledger_events,record_events,accepted_intervals,command_receipts,outbox TO facts_runtime;
GRANT UPDATE(revision) ON tenant_revisions TO facts_runtime;
GRANT UPDATE(stream_revision) ON records TO facts_runtime;
GRANT UPDATE(ledger_revision) ON accepted_intervals TO facts_runtime;
REVOKE UPDATE, DELETE, TRUNCATE ON policy_versions,predicate_versions,evidence,record_versions,version_evidence,attestations,attestation_events,ledger_events,record_events,command_receipts FROM facts_runtime;
GRANT SELECT,INSERT ON evidence_objects TO facts_runtime;
GRANT SELECT,INSERT,UPDATE ON active_predicates TO facts_runtime;
