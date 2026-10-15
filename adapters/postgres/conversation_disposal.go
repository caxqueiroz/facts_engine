package postgres

import (
	"context"
	"errors"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (t *transaction) DisposeConversation(ctx context.Context, r model.ConversationDisposalRequest) error {
	q := ledgerdb.New(t.tx)
	h, err := q.ConversationDisposalHeader(ctx, ledgerdb.ConversationDisposalHeaderParams{TenantID: t.tenant, ID: r.ConversationID})
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrDenied
	}
	if err != nil {
		return err
	}
	if h.ScopeID != r.Scope || !h.RecordedAt.Valid || h.RecordedAt.InfinityModifier != pgtype.Finite {
		return model.ErrDenied
	}
	if err := t.lockRetentionScope(ctx, r.Scope); err != nil {
		return err
	}
	if err := q.LockConversationGuard(ctx, ledgerdb.LockConversationGuardParams{TenantID: t.tenant, ConversationID: r.ConversationID}); err != nil {
		return err
	}
	payload, err := q.GetConversationPayload(ctx, ledgerdb.GetConversationPayloadParams{TenantID: t.tenant, ID: r.ConversationID})
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return model.ErrPrecondition
	}
	var conversation model.ConversationRecord
	if err := t.decode(payload, &conversation); err != nil {
		return err
	}
	if conversation.PayloadDisposed || conversation.ID != r.ConversationID || conversation.Tenant != t.tenant || conversation.Request.Scope != r.Scope || conversation.Digest != model.ConversationDigest(conversation.Request) {
		return model.ErrPrecondition
	}
	policy, err := t.RetentionPolicy(ctx, r.Scope, r.PolicyID)
	if err != nil {
		return err
	}
	if policy.ID != r.PolicyID || policy.Scope != r.Scope || policy.RecordClass != "conversation_payload" || policy.Purpose != r.Purpose || policy.Jurisdiction != r.Jurisdiction {
		return model.ErrPrecondition
	}
	holds, err := q.ListActiveLegalHolds(ctx, ledgerdb.ListActiveLegalHoldsParams{TenantID: t.tenant, ScopeID: r.Scope})
	if err != nil {
		return err
	}
	if len(holds) > 0 {
		return model.ErrPrecondition
	}
	mature, err := q.EvidenceAgeMature(ctx, ledgerdb.EvidenceAgeMatureParams{RegisteredAt: h.RecordedAt, MinimumAgeSeconds: policy.MinimumAgeSeconds})
	if err != nil {
		return err
	}
	if !mature {
		return model.ErrPrecondition
	}
	blocked, err := q.ConversationDisposalBlocked(ctx, ledgerdb.ConversationDisposalBlockedParams{TenantID: t.tenant, ConversationID: r.ConversationID, ScopeID: r.Scope, LedgerRevision: h.LedgerRevision})
	if err != nil {
		return err
	}
	if !blocked.Valid || blocked.Bool {
		return model.ErrPrecondition
	}
	marker, err := t.encode(model.ConversationTombstone(conversation))
	if err != nil {
		return err
	}
	if err = q.InsertConversationDisposal(ctx, ledgerdb.InsertConversationDisposalParams{TenantID: t.tenant, ConversationID: r.ConversationID, Payload: marker}); err != nil {
		return err
	}
	removed, err := q.DeleteConversationPayload(ctx, ledgerdb.DeleteConversationPayloadParams{TenantID: t.tenant, ConversationID: r.ConversationID})
	if err != nil {
		return err
	}
	if removed != 1 {
		return model.ErrPrecondition
	}
	return nil
}
