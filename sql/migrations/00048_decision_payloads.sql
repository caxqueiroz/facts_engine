-- +goose Up
-- Stop writers and upgrade readers/workers together. Preserve ciphertext exactly.
CREATE TABLE decision_payloads (
 tenant_id text NOT NULL,decision_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,decision_id),
 FOREIGN KEY(tenant_id,decision_id) REFERENCES decision_receipts(tenant_id,id)
);
ALTER TABLE decision_receipts NO FORCE ROW LEVEL SECURITY;
INSERT INTO decision_payloads(tenant_id,decision_id,payload)
SELECT tenant_id,id,payload FROM decision_receipts;
ALTER TABLE decision_receipts FORCE ROW LEVEL SECURITY;
ALTER TABLE decision_receipts DROP COLUMN payload;
ALTER TABLE decision_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE decision_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON decision_payloads
USING(tenant_id=current_setting('app.tenant_id',true))
WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'decision payload separation is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
