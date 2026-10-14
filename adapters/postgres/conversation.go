package postgres

import (
	"context"
	"math"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) Conversation(ctx context.Context, id string) (model.ConversationRecord, error) {
	var r model.ConversationRecord
	b, err := ledgerdb.New(t.tx).GetConversationPayload(ctx, ledgerdb.GetConversationPayloadParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return r, translate(err)
	}
	err = t.decode(b, &r)
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
	if err = ledgerdb.New(t.tx).InsertConversationPayload(ctx, ledgerdb.InsertConversationPayloadParams{TenantID: t.tenant, ID: r.ID, ScopeID: r.Request.Scope, LedgerRevision: e.LedgerRevision, Payload: b}); err != nil {
		return err
	}
	for ordinal, id := range r.EvidenceIDs {
		if ordinal > math.MaxInt32 {
			return model.ErrInvalid
		}
		if err = ledgerdb.New(t.tx).InsertConversationMessage(ctx, ledgerdb.InsertConversationMessageParams{TenantID: t.tenant, ConversationID: r.ID, Ordinal: int32(ordinal), EvidenceID: id}); err != nil {
			return err
		}
	}
	return nil
}
