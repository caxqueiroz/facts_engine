-- +goose Up
CREATE INDEX pending_evidence_cleanup_cursor ON pending_evidence_uploads
(tenant_id,scope_id,policy_id,locator COLLATE "C",object_version COLLATE "C")
INCLUDE(uploaded_at,byte_size);
-- +goose Down
DROP INDEX pending_evidence_cleanup_cursor;
