package services

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestMemoriesPageTokenBinding(t *testing.T) {
	s := &Service{reads: model.ReadOptions{PaginationKey: bytes.Repeat([]byte{9}, 32)}}
	c := model.Caller{Tenant: "org", Principal: "reader"}
	now := time.Now().UTC()
	q := model.MemoriesQuery{Scope: "case", Purpose: "consolidate", ValidAt: now, Limit: 1}
	p := factPageToken{Version: memoriesPageVersion, CallerDigest: model.Digest(c), QueryDigest: memoriesQueryDigest(q), After: "record", Revision: 5, EvaluatedAt: now, ExpiresAt: now.Add(time.Minute)}
	signed := s.signFactPage(p)
	if _, err := s.parseMemoryPage(signed, c, q, now); err != nil {
		t.Fatal(err)
	}
	continued := q
	continued.PageToken = signed
	if _, err := s.parseMemoryPage(signed, c, continued, now); err != nil {
		t.Fatal("page token must not bind itself", err)
	}
	if _, err := s.parseMemoryPage(signed, c, q, now.Add(time.Minute)); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("expired token accepted", err)
	}
	for _, other := range []model.Caller{{Tenant: "other", Principal: c.Principal}, {Tenant: c.Tenant, Principal: "other"}} {
		if _, err := s.parseMemoryPage(signed, other, q, now); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("token replayed by another caller", other, err)
		}
	}
	known := now.Add(-time.Hour)
	for name, change := range map[string]func(*model.MemoriesQuery){
		"scope":    func(q *model.MemoriesQuery) { q.Scope = "other" },
		"subtype":  func(q *model.MemoriesQuery) { q.Subtype = "summary" },
		"purpose":  func(q *model.MemoriesQuery) { q.Purpose = "other" },
		"valid at": func(q *model.MemoriesQuery) { q.ValidAt = now.Add(time.Second) },
		"revision": func(q *model.MemoriesQuery) { q.KnowledgeRevision = 3 },
		"known at": func(q *model.MemoriesQuery) { q.KnownAt = &known },
		"limit":    func(q *model.MemoriesQuery) { q.Limit = 2 },
	} {
		changed := q
		change(&changed)
		if _, err := s.parseMemoryPage(signed, c, changed, now); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("token replayed with a different query:", name, err)
		}
	}
	facts := p
	facts.Version = "facts-page-v1"
	if _, err := s.parseMemoryPage(s.signFactPage(facts), c, q, now); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("fact page token accepted for memories", err)
	}
	for _, raw := range []string{"", signed + "x", "not.a.signature"} {
		if _, err := s.parseMemoryPage(raw, c, q, now); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("invalid token accepted", err)
		}
	}
	s.reads.PaginationKey = bytes.Repeat([]byte{8}, 32)
	if _, err := s.parseMemoryPage(signed, c, q, now); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("rotated key token", err)
	}
}

func TestListMemoriesValidation(t *testing.T) {
	s := &Service{reads: model.ReadOptions{PaginationKey: bytes.Repeat([]byte{9}, 32)}}
	c := model.Caller{Tenant: "org", Principal: "reader"}
	now := time.Now().UTC()
	valid := model.MemoriesQuery{Scope: "case", Purpose: "consolidate", ValidAt: now, Limit: 1}
	zero := time.Time{}
	for name, change := range map[string]func(*model.MemoriesQuery){
		"scope":        func(q *model.MemoriesQuery) { q.Scope = "" },
		"subtype":      func(q *model.MemoriesQuery) { q.Subtype = "bad subtype" },
		"valid at":     func(q *model.MemoriesQuery) { q.ValidAt = time.Time{} },
		"limit low":    func(q *model.MemoriesQuery) { q.Limit = 0 },
		"limit high":   func(q *model.MemoriesQuery) { q.Limit = 101 },
		"revision":     func(q *model.MemoriesQuery) { q.KnowledgeRevision = -1 },
		"zero known":   func(q *model.MemoriesQuery) { q.KnownAt = &zero },
		"both history": func(q *model.MemoriesQuery) { q.KnownAt = &now; q.KnowledgeRevision = 1 },
	} {
		q := valid
		change(&q)
		if _, err := s.ListMemories(context.Background(), c, q); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("invalid query accepted:", name, err)
		}
	}
	unkeyed := &Service{}
	if _, err := unkeyed.ListMemories(context.Background(), c, valid); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("listing without a pagination key", err)
	}
}
