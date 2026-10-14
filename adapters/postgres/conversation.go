package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) Conversation(ctx context.Context, id string) (model.ConversationRecord, error) {
	var r model.ConversationRecord
	err := t.read(ctx, &r, `SELECT payload FROM conversations WHERE tenant_id=$1 AND id=$2`, t.tenant, id)
	return r, err
}
func (t *transaction) StageConversation(ctx context.Context, r model.ConversationRecord) error {
	if r.Tenant != t.tenant {
		return model.ErrInvalid
	}
	for _, id := range r.EvidenceIDs {
		if err := t.guardEvidenceReference(ctx, id); err != nil {
			return err
		}
	}
	t.pendingConversation = &r
	return nil
}
func (t *transaction) flushConversation(ctx context.Context, e model.Event) error {
	if t.pendingConversation == nil {
		return nil
	}
	r := *t.pendingConversation
	r.LedgerRevision = e.LedgerRevision
	b, err := t.encode(r)
	if err != nil {
		return err
	}
	if _, err = t.tx.Exec(ctx, `INSERT INTO conversations VALUES($1,$2,$3,$4,$5)`, t.tenant, r.ID, r.Request.Scope, e.LedgerRevision, b); err != nil {
		return err
	}
	for ordinal, id := range r.EvidenceIDs {
		if _, err = t.tx.Exec(ctx, `INSERT INTO conversation_messages VALUES($1,$2,$3,$4)`, t.tenant, r.ID, ordinal, id); err != nil {
			return err
		}
	}
	return nil
}
