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
-- Dedicated worker credentials cannot append business events or change facts.
DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='facts_worker') THEN CREATE ROLE facts_worker NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;
GRANT USAGE ON SCHEMA public TO facts_worker;
GRANT SELECT ON ledger_events,outbox,tenant_revisions TO facts_worker;
GRANT SELECT,INSERT,UPDATE ON consumer_offsets TO facts_worker;
GRANT SELECT,INSERT ON consumer_retry_commands TO facts_worker;
GRANT SELECT ON consumer_offsets TO facts_runtime;
-- Projection/archive consumers resolve immutable source content with their own login.
DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='facts_projector') THEN CREATE ROLE facts_projector NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;
DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='facts_archiver') THEN CREATE ROLE facts_archiver NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;
GRANT facts_worker TO facts_projector,facts_archiver;
GRANT SELECT ON record_versions,predicate_versions,policy_versions,evidence,timeline_generations,timeline_history,accepted_intervals TO facts_projector,facts_archiver;
GRANT SELECT ON fact_snapshots,calculation_receipts,attestations,audit_exports TO facts_archiver;
GRANT SELECT ON active_projections TO facts_runtime;
DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='facts_rebuild') THEN CREATE ROLE facts_rebuild NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;
GRANT facts_projector TO facts_rebuild;
GRANT SELECT ON record_events TO facts_rebuild;
-- UPDATE is needed for row locks at the tenant revision barrier; this is an
-- administrative role, never an end-user runtime identity.
GRANT UPDATE ON tenant_revisions TO facts_rebuild;
GRANT SELECT,INSERT,UPDATE ON projection_generations,active_projections TO facts_rebuild;
GRANT SELECT,INSERT ON projection_rebuild_events TO facts_rebuild;
GRANT INSERT ON tenant_revisions TO facts_rebuild;
GRANT SELECT,INSERT ON context_manifests,context_scopes,context_items,decision_receipts,decision_scopes,decision_items,decision_artifacts TO facts_runtime;
GRANT SELECT ON context_manifests,context_scopes,context_items,decision_receipts,decision_scopes,decision_items,decision_artifacts TO facts_archiver;
-- Retention metadata is readable by engine/archiver; privileged changes use
-- separate database and OpenFGA roles, never ordinary runtime credentials.
GRANT SELECT ON retention_domain TO facts_runtime,facts_archiver;
GRANT SELECT ON retention_events,retention_policies,legal_holds,legal_hold_releases,evidence_retention,retention_objects,retention_scopes TO facts_runtime,facts_archiver;
GRANT INSERT ON retention_scopes,retention_objects,evidence_retention TO facts_runtime;
GRANT UPDATE(epoch) ON retention_scopes,retention_objects TO facts_runtime;
DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='facts_retention') THEN CREATE ROLE facts_retention NOLOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$;
GRANT facts_runtime TO facts_retention;
GRANT INSERT ON retention_events,retention_policies,legal_holds,legal_hold_releases TO facts_retention;
GRANT UPDATE(disposed) ON retention_objects TO facts_retention;
GRANT DELETE ON evidence_objects TO facts_retention;
GRANT SELECT,INSERT ON conversations,conversation_messages TO facts_runtime;
GRANT SELECT ON conversations,conversation_messages TO facts_archiver;
GRANT SELECT,INSERT ON extraction_jobs,extraction_job_state,extraction_retry_events TO facts_runtime;
GRANT UPDATE ON extraction_job_state TO facts_runtime;
GRANT SELECT ON extraction_jobs,extraction_job_state,extraction_retry_events TO facts_archiver;
GRANT SELECT,INSERT ON extraction_quota,extraction_attempts,extraction_results,extraction_proposals TO facts_runtime;
GRANT UPDATE ON extraction_quota TO facts_runtime;
GRANT SELECT ON extraction_attempts,extraction_results,extraction_proposals TO facts_archiver;

GRANT SELECT,INSERT ON predicate_migrations TO facts_runtime;
GRANT SELECT ON predicate_migrations TO facts_archiver;

GRANT SELECT,INSERT ON memory_types,active_memory_types,memory_versions,memory_evidence,memory_dependencies,memory_record_events TO facts_runtime;
GRANT UPDATE ON active_memory_types TO facts_runtime;
GRANT SELECT ON memory_types,memory_versions,memory_evidence,memory_dependencies,memory_record_events TO facts_archiver;

GRANT SELECT,INSERT ON memory_timeline_generations,memory_timeline_history,memory_rejections TO facts_runtime;
GRANT SELECT ON memory_timeline_generations,memory_timeline_history,memory_rejections TO facts_archiver;

GRANT SELECT,INSERT ON attestation_identities,memory_attestations,memory_attestation_events TO facts_runtime;
GRANT SELECT ON attestation_identities,memory_attestations,memory_attestation_events TO facts_archiver;

GRANT SELECT ON memory_versions,memory_types,memory_timeline_generations,memory_timeline_history,memory_record_events TO facts_projector;

GRANT SELECT,INSERT ON context_memory_items,decision_memory_items TO facts_runtime;
GRANT SELECT ON context_memory_items,decision_memory_items TO facts_archiver;
