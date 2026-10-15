package services

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/model"
	"strings"
	"time"
)

type factPageToken struct {
	StreamRevision                            int64 `json:",omitempty"`
	Version, CallerDigest, QueryDigest, After string
	Revision                                  int64
	EvaluatedAt, ExpiresAt                    time.Time
}

func NewWithReads(l model.Ledger, a model.Authorizer, e model.EvidenceStore, options model.ReadOptions) (*Service, error) {
	if len(options.PaginationKey) != 32 {
		return nil, model.ErrInvalid
	}
	if options.TokenLifetime == 0 {
		options.TokenLifetime = 15 * time.Minute
	}
	if options.TokenLifetime < time.Second || options.TokenLifetime > time.Hour {
		return nil, model.ErrInvalid
	}
	s, err := New(l, a, e)
	if err != nil {
		return nil, err
	}
	options.PaginationKey = append([]byte(nil), options.PaginationKey...)
	s.reads = options
	return s, nil
}
func pageQueryDigest(q model.FactsQuery) string {
	q.PageToken = ""
	q.ValidAt = q.ValidAt.UTC()
	if q.KnownAt != nil {
		at := q.KnownAt.UTC()
		q.KnownAt = &at
	}
	return model.Digest(q)
}
func (s *Service) signFactPage(p factPageToken) string {
	b, _ := json.Marshal(p)
	mac := hmac.New(sha256.New, s.reads.PaginationKey)
	mac.Write(b)
	return base64.RawURLEncoding.EncodeToString(b) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func (s *Service) parseFactPage(raw string, c model.Caller, q model.FactsQuery, now time.Time) (factPageToken, error) {
	return s.parsePage(raw, c, "facts-page-v1", pageQueryDigest(q), now)
}
func (s *Service) parsePage(raw string, c model.Caller, version, queryDigest string, now time.Time) (factPageToken, error) {
	var token factPageToken
	if len(raw) > 4096 {
		return token, model.ErrInvalid
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 2 {
		return token, model.ErrInvalid
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return token, model.ErrInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return token, model.ErrInvalid
	}
	mac := hmac.New(sha256.New, s.reads.PaginationKey)
	mac.Write(body)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return token, model.ErrInvalid
	}
	if err = json.Unmarshal(body, &token); err != nil {
		return token, model.ErrInvalid
	}
	if token.Version != version || token.CallerDigest != model.Digest(c) || token.QueryDigest != queryDigest || !model.ValidID(token.After) || token.Revision < 1 || token.EvaluatedAt.IsZero() || !token.ExpiresAt.After(now) {
		return factPageToken{}, model.ErrInvalid
	}
	return token, nil
}
func (s *Service) ListFacts(ctx context.Context, c model.Caller, q model.FactsQuery) (model.FactsPage, error) {
	out := model.FactsPage{Items: []model.SnapshotItem{}}
	if len(s.reads.PaginationKey) != 32 {
		return out, model.ErrPrecondition
	}
	if !model.ValidID(q.Scope) || (q.Subject != "" && !model.ValidID(q.Subject)) || (q.Predicate != "" && !model.ValidID(q.Predicate)) || q.ValidAt.IsZero() || q.Limit < 1 || q.Limit > 100 || q.KnowledgeRevision < 0 || (q.KnownAt != nil && (q.KnownAt.IsZero() || q.KnowledgeRevision != 0)) {
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
	token := factPageToken{Version: "facts-page-v1", CallerDigest: model.Digest(c), QueryDigest: pageQueryDigest(q), ExpiresAt: now.Add(s.reads.TokenLifetime)}
	if q.PageToken != "" {
		var err error
		token, err = s.parseFactPage(q.PageToken, c, q, now)
		if err != nil {
			return out, err
		}
	}
	more := false
	after := ""
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, ok := tx.(model.ReadTx)
		if !ok {
			return model.ErrPrecondition
		}
		out.Items = []model.SnapshotItem{}
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
		records, err := t.FactRecords(ctx, q, out.LedgerRevision, token.After, q.Limit+1)
		if err != nil {
			return err
		}
		more = len(records) > q.Limit
		if more {
			records = records[:q.Limit]
		}
		for _, record := range records {
			item, err := s.resolve(ctx, tx, model.FactQuery{Key: record.Key, ValidAt: q.ValidAt}, out.LedgerRevision, out.EvaluatedAt, true)
			if err != nil {
				return err
			}
			out.Items = append(out.Items, item)
			after = record.ID
		}
		return nil
	})
	if err != nil {
		return model.FactsPage{}, err
	}
	for i := range out.Items {
		item := &out.Items[i]
		if len(item.EvidenceIDs) > 0 {
			if _, err = s.loadEvidence(ctx, c, q.Scope, item.EvidenceIDs); err != nil {
				if interrupted := evidenceDependencyFailure(ctx, err); interrupted != nil {
					return model.FactsPage{}, interrupted
				}
				item.Resolution.State = "ineligible"
				item.Resolution.Value = nil
			}
		}
	}
	if err = authorize(); err != nil {
		return model.FactsPage{}, err
	}
	if more {
		token.After = after
		token.Revision = out.LedgerRevision
		token.EvaluatedAt = out.EvaluatedAt
		out.NextPageToken = s.signFactPage(token)
	}
	return out, nil
}
func readSelection(ctx context.Context, tx model.Tx, revision int64, knownAt *time.Time) (int64, time.Time, error) {
	if knownAt == nil {
		return selection(ctx, tx, revision)
	}
	t, ok := tx.(model.ReadTx)
	if !ok {
		return 0, time.Time{}, model.ErrPrecondition
	}
	rev, err := t.RevisionAt(ctx, *knownAt)
	if err != nil {
		return 0, time.Time{}, err
	}
	// Zero is an empty historical view, not the API's "latest" revision sentinel.
	if rev == 0 {
		return 0, knownAt.UTC(), nil
	}
	_, at, err := selection(ctx, tx, rev)
	return rev, at, err
}
