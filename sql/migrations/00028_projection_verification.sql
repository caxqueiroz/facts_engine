-- +goose Up
ALTER TABLE projection_generations ADD COLUMN verification_version text NOT NULL DEFAULT '' CHECK(verification_version IN ('','projection-inventory-v2'));
CREATE INDEX record_events_projection_inventory ON record_events(tenant_id,version_id COLLATE "C",event_id);
CREATE INDEX memory_events_projection_inventory ON memory_record_events(tenant_id,version_id COLLATE "C",event_id);
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'projection verification receipts are forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
