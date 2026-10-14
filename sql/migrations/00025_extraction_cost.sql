-- +goose Up
ALTER TABLE extraction_quota ADD COLUMN cost_unit text NOT NULL DEFAULT '';
ALTER TABLE extraction_quota ADD COLUMN reserved_cost_units bigint NOT NULL DEFAULT 0 CHECK(reserved_cost_units>=0);
ALTER TABLE extraction_quota ADD CONSTRAINT extraction_cost_unit CHECK(cost_unit<>'' OR reserved_cost_units=0);
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'cost reservation accounting is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
