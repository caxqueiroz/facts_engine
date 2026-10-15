package postgres

import "context"

// The epoch write makes an old repeatable-read archive transaction abort if
// content disposal committed after its snapshot. Payload/tombstone inserts use
// the same guard through database triggers.
func (t *transaction) lockVersionContent(ctx context.Context, memory bool, id string) error {
	query := `INSERT INTO fact_version_guards(tenant_id,version_id,epoch) VALUES($1,$2,1) ON CONFLICT(tenant_id,version_id) DO UPDATE SET epoch=fact_version_guards.epoch+1`
	if memory {
		query = `INSERT INTO memory_version_guards(tenant_id,version_id,epoch) VALUES($1,$2,1) ON CONFLICT(tenant_id,version_id) DO UPDATE SET epoch=memory_version_guards.epoch+1`
	}
	_, err := t.tx.Exec(ctx, query, t.tenant, id)
	return err
}
