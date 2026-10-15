-- +goose Up
-- Snapshot identity, ordering, digest and source inventory remain immutable.
-- Deploy with writers stopped; this migration moves ciphertext without decrypting it.
CREATE TABLE fact_snapshot_payloads (
 tenant_id text NOT NULL,snapshot_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,snapshot_id),
 FOREIGN KEY(tenant_id,snapshot_id) REFERENCES fact_snapshots(tenant_id,id)
);
ALTER TABLE fact_snapshots NO FORCE ROW LEVEL SECURITY;
INSERT INTO fact_snapshot_payloads(tenant_id,snapshot_id,payload)
SELECT tenant_id,id,payload FROM fact_snapshots;
ALTER TABLE fact_snapshots FORCE ROW LEVEL SECURITY;
ALTER TABLE fact_snapshots DROP COLUMN payload;
ALTER TABLE fact_snapshot_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE fact_snapshot_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON fact_snapshot_payloads
USING(tenant_id=current_setting('app.tenant_id',true))
WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'snapshot payload separation is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
