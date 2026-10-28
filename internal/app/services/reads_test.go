package services

import (
	"bytes"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

func TestPageTokenBindingAndExpiry(t *testing.T) {
	s := &Service{reads: model.ReadOptions{PaginationKey: bytes.Repeat([]byte{9}, 32)}}
	c := model.Caller{Tenant: "org", Principal: "reader"}
	now := time.Now().UTC()
	q := model.FactsQuery{Scope: "case", Purpose: "review", ValidAt: now, Limit: 1}
	p := factPageToken{Version: "facts-page-v1", CallerDigest: model.Digest(c), QueryDigest: pageQueryDigest(q), After: "record", Revision: 5, EvaluatedAt: now, ExpiresAt: now.Add(time.Minute)}
	signed := s.signFactPage(p)
	if _, err := s.parseFactPage(signed, c, q, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.parseFactPage(signed, c, q, now.Add(time.Minute)); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("expired token accepted", err)
	}
	other := c
	other.Tenant = "other"
	if _, err := s.parseFactPage(signed, other, q, now); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("cross tenant token", err)
	}
	other = c
	other.Principal = "other"
	if _, err := s.parseFactPage(signed, other, q, now); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("cross principal token", err)
	}
	changed := q
	changed.Purpose = "other"
	if _, err := s.parseFactPage(signed, c, changed, now); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("changed purpose", err)
	}
	for _, raw := range []string{"", signed + "x", "not.a.signature"} {
		if _, err := s.parseFactPage(raw, c, q, now); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("invalid token accepted", err)
		}
	}
	s.reads.PaginationKey = bytes.Repeat([]byte{8}, 32)
	if _, err := s.parseFactPage(signed, c, q, now); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("rotated key token", err)
	}
}
