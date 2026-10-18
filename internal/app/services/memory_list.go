package services

import (
	"context"
	"errors"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

const memoriesPageVersion = "memories-page-v1"

func memoriesQueryDigest(q model.MemoriesQuery) string {
	q.PageToken = ""
	q.ValidAt = q.ValidAt.UTC()
	if q.KnownAt != nil {
		at := q.KnownAt.UTC()
		q.KnownAt = &at
	}
	return model.Digest(q)
}
func (s *Service) parseMemoryPage(raw string, c model.Caller, q model.MemoriesQuery, now time.Time) (factPageToken, error) {
	return s.parsePage(raw, c, memoriesPageVersion, memoriesQueryDigest(q), now)
}

// ListMemories pages through one scope's memory records in record-ID order and
// returns each one as GetMemory would resolve it at the pinned revision. Records
// without a version selected at ValidAt (pending, rejected, expired) and
// retracted memories are skipped, so a page may hold fewer than Limit items, or
// none, and still carry a NextPageToken.
func (s *Service) ListMemories(ctx context.Context, c model.Caller, q model.MemoriesQuery) (model.MemoriesPage, error) {
	out := model.MemoriesPage{Items: []model.MemoryListItem{}}
	if len(s.reads.PaginationKey) != 32 {
		return out, model.ErrPrecondition
	}
	if !model.ValidID(q.Scope) || (q.Subtype != "" && !model.ValidID(q.Subtype)) || q.ValidAt.IsZero() || q.Limit < 1 || q.Limit > 100 || q.KnowledgeRevision < 0 || (q.KnownAt != nil && (q.KnownAt.IsZero() || q.KnowledgeRevision != 0)) {
		return out, model.ErrInvalid
	}
	historical := q.KnowledgeRevision > 0 || q.KnownAt != nil
	authorize := func() error {
		_, err := s.snapshotAuthorization(ctx, c, []model.FactKey{{Scope: q.Scope}}, model.Command{Purpose: q.Purpose}, historical, false)
		return err
	}
	if err := authorize(); err != nil {
		return out, err
	}
	now := time.Now().UTC()
	token := factPageToken{Version: memoriesPageVersion, CallerDigest: model.Digest(c), QueryDigest: memoriesQueryDigest(q), ExpiresAt: now.Add(s.reads.TokenLifetime)}
	if q.PageToken != "" {
		var err error
		token, err = s.parseMemoryPage(q.PageToken, c, q, now)
		if err != nil {
			return out, err
		}
	}
	more := false
	after := ""
	views := []memoryRead{}
	keys := []model.MemoryKey{}
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, ok := tx.(model.ReadTx)
		if !ok {
			return model.ErrPrecondition
		}
		views, keys = []memoryRead{}, []model.MemoryKey{}
		more = false
		after = ""
		if q.PageToken != "" {
			out.LedgerRevision = token.Revision
			out.EvaluatedAt = token.EvaluatedAt
			head, err := tx.Revision(ctx)
			if err != nil {
				return err
			}
			if head < token.Revision {
				return model.ErrPrecondition
			}
		} else {
			var err error
			out.LedgerRevision, out.EvaluatedAt, err = readSelection(ctx, tx, q.KnowledgeRevision, q.KnownAt)
			if err != nil {
				return err
			}
		}
		if out.LedgerRevision == 0 {
			return nil
		}
		records, err := t.MemoryRecords(ctx, q, out.LedgerRevision, token.After, q.Limit+1)
		if err != nil {
			return err
		}
		more = len(records) > q.Limit
		if more {
			records = records[:q.Limit]
		}
		for _, record := range records {
			// The cursor advances past every fetched record, including skipped ones.
			after = record.ID
			key := model.MemoryKey{Scope: record.Key.Scope, ID: record.Key.Discriminator}
			if record.Key.Scope != q.Scope || model.MemoryRecordID(key) != record.ID {
				return model.ErrPrecondition
			}
			// Every page reads at a pinned revision, so resolution is historical, as in ListFacts.
			view, err := s.resolveMemory(ctx, tx, model.MemoryQuery{Key: key, ValidAt: q.ValidAt}, out.LedgerRevision, out.EvaluatedAt, true)
			if err != nil {
				return err
			}
			if view.Version == nil || view.Result.State == "retracted" || (q.Subtype != "" && view.Version.Proposal.Subtype != q.Subtype) {
				continue
			}
			views = append(views, view)
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return model.MemoriesPage{}, err
	}
	for i, view := range views {
		query := model.MemoryQuery{Key: keys[i], Purpose: q.Purpose, ValidAt: q.ValidAt, KnowledgeRevision: q.KnowledgeRevision, KnownAt: q.KnownAt}
		resolved, err := s.discloseMemory(ctx, c, query, view)
		// As in RecallMemories, a memory whose dependency scopes or evidence the
		// caller cannot read is left out rather than failing the whole page.
		if errors.Is(err, model.ErrDenied) || errors.Is(err, model.ErrPrecondition) {
			continue
		}
		if err != nil {
			return model.MemoriesPage{}, err
		}
		out.Items = append(out.Items, model.MemoryListItem{Key: keys[i], Memory: resolved})
	}
	if err = authorize(); err != nil {
		return model.MemoriesPage{}, err
	}
	if more {
		token.After = after
		token.Revision = out.LedgerRevision
		token.EvaluatedAt = out.EvaluatedAt
		out.NextPageToken = s.signFactPage(token)
	}
	return out, nil
}
