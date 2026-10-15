-- +goose Up
-- Stop writers and upgrade readers/workers together. Preserve ciphertext exactly.
CREATE TABLE calculation_payloads (
 tenant_id text NOT NULL,calculation_id text NOT NULL,payload bytea NOT NULL,
 PRIMARY KEY(tenant_id,calculation_id),
 FOREIGN KEY(tenant_id,calculation_id) REFERENCES calculation_receipts(tenant_id,id)
);
ALTER TABLE calculation_receipts NO FORCE ROW LEVEL SECURITY;
INSERT INTO calculation_payloads(tenant_id,calculation_id,payload)
SELECT tenant_id,id,payload FROM calculation_receipts;
ALTER TABLE calculation_receipts FORCE ROW LEVEL SECURITY;
ALTER TABLE calculation_receipts DROP COLUMN payload;
ALTER TABLE calculation_payloads ENABLE ROW LEVEL SECURITY;
ALTER TABLE calculation_payloads FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_boundary ON calculation_payloads
USING(tenant_id=current_setting('app.tenant_id',true))
WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN RAISE EXCEPTION 'calculation payload separation is forward-only; restore a verified backup'; END $$;
-- +goose StatementEnd
