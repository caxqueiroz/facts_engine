-- +goose Up
-- A cleanup target must resolve to exactly one original disposal authorization.
-- Existing duplicate authorizations require investigation rather than choosing
-- an arbitrary event. This index also bounds the scheduler's event lookup.
CREATE UNIQUE INDEX retention_version_disposal_identity ON retention_events(tenant_id,kind,target_id)
WHERE kind IN ('dispose_fact_payload','dispose_memory_payload');
-- +goose Down
DROP INDEX retention_version_disposal_identity;
