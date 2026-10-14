package services

import (
	"context"
	"strconv"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) ListFactHistory(ctx context.Context, c model.Caller, q model.HistoryPageQuery) (model.HistoryPage, error) {
	empty := model.HistoryPage{}
	if len(s.reads.PaginationKey) != 32 {
		return empty, model.ErrPrecondition
	}
	if !model.ValidID(q.RecordID) || !model.ValidID(q.Scope) || q.Limit < 1 || q.Limit > 100 {
		return empty, model.ErrInvalid
	}
	authorize := func() error {
		_, err := s.authorize(ctx, c, q.Scope, "can_read_history", model.Command{Purpose: q.Purpose})
		return err
	}
	if err := authorize(); err != nil {
		return empty, err
	}
	binding := q
	binding.PageToken = ""
	now := time.Now().UTC()
	token := factPageToken{Version: "fact-history-page-v1", CallerDigest: model.Digest(c), QueryDigest: model.Digest(binding), ExpiresAt: now.Add(s.reads.TokenLifetime), EvaluatedAt: now}
	after := int64(0)
	if q.PageToken != "" {
		var err error
		token, err = s.parsePage(q.PageToken, c, token.Version, token.QueryDigest, now)
		if err != nil {
			return empty, err
		}
		after, err = strconv.ParseInt(token.After, 10, 64)
		if err != nil || after < 1 || after >= token.StreamRevision {
			return empty, model.ErrInvalid
		}
	}
	out := model.HistoryPage{}
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, ok := tx.(model.HistoryPageTx)
		if !ok {
			return model.ErrPrecondition
		}
		record, err := tx.Record(ctx, q.RecordID)
		if err != nil {
			return err
		}
		if record.Key.Scope != q.Scope {
			return model.ErrDenied
		}
		head, err := tx.Revision(ctx)
		if err != nil {
			return err
		}
		if q.PageToken == "" {
			token.Revision = head
			token.StreamRevision = record.Revision
		}
		if head < token.Revision || record.Revision < token.StreamRevision {
			return model.ErrPrecondition
		}
		events, err := t.HistoryPage(ctx, q.RecordID, token.Revision, after, q.Limit)
		if err != nil {
			return err
		}
		if len(events) > q.Limit {
			return model.ErrPrecondition
		}
		expected := after
		for _, event := range events {
			expected++
			if event.RecordID != q.RecordID || event.Scope != q.Scope || event.StreamRevision != expected || event.LedgerRevision > token.Revision || event.StreamRevision > token.StreamRevision {
				return model.ErrPrecondition
			}
		}
		// Detect retained-history gaps instead of returning a silently shortened page.
		if int64(len(events)) != min(int64(q.Limit), token.StreamRevision-after) {
			return model.ErrPrecondition
		}
		out = model.HistoryPage{Events: events, LedgerRevision: token.Revision, StreamRevision: token.StreamRevision}
		return nil
	})
	if err != nil {
		return empty, err
	}
	if err = authorize(); err != nil {
		return empty, err
	}
	if after+int64(len(out.Events)) < out.StreamRevision {
		token.After = strconv.FormatInt(after+int64(len(out.Events)), 10)
		out.NextPageToken = s.signFactPage(token)
	}
	return out, nil
}
