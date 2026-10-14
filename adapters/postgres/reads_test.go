package postgres_test

import (
	"bytes"
	"context"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

func testFactListing(t *testing.T, ctx context.Context, l model.Ledger, store *evidence.Postgres) {
	t.Helper()
	options := model.ReadOptions{PaginationKey: bytes.Repeat([]byte{7}, 32)}
	engine, err := facts.NewWithOptions(l, allow{}, store, model.EngineOptions{Reads: options})
	if err != nil {
		t.Fatal(err)
	}
	c := model.Caller{Tenant: "listing-bank", Principal: "proposer"}
	reviewer := c
	reviewer.Principal = "reviewer"
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "list-test"} }
	_, err = engine.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{ID: "manual", Scope: "case", Mode: "manual", SourceClasses: []string{"bank_record"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: cmd("schema"), Definition: model.PredicateVersion{Scope: "case", Predicate: "color", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(ctx, c.Tenant, "source", []byte("blue"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: cmd("evidence"), Evidence: model.Evidence{ID: "evidence", Scope: "case", SourceSystem: "app", SourceObject: "source", SourceRevision: "v1", Speaker: "staff", SourceClass: "bank_record", SourceTime: time.Now(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "test", Object: ref}})
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	propose := func(subject string) model.CommitResult {
		t.Helper()
		value := "blue"
		r, err := engine.ProposeFact(ctx, c, model.FactProposal{Command: cmd("propose-" + subject), Key: model.FactKey{Scope: "case", Subject: subject, Predicate: "color"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &from}, Sources: []model.SourceSpan{{EvidenceID: "evidence", Start: 0, End: 4, Quote: "blue"}}})
		if err != nil {
			t.Fatal(err)
		}
		r, err = engine.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("approve-" + subject), Scope: "case", RecordID: r.RecordID, VersionID: r.VersionID, ExpectedRevision: r.StreamRevision})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	propose("one")
	propose("two")
	q := model.FactsQuery{Scope: "case", Purpose: "review", ValidAt: from, Limit: 1}
	first, err := engine.ListFacts(ctx, c, q)
	if err != nil || len(first.Items) != 1 || first.NextPageToken == "" {
		t.Fatal(first, err)
	}
	marker := time.Now().UTC()
	newRecord := propose("three")
	next := q
	next.PageToken = first.NextPageToken
	second, err := engine.ListFacts(ctx, c, next)
	if err != nil || len(second.Items) != 1 || second.NextPageToken != "" || second.LedgerRevision != first.LedgerRevision || !second.EvaluatedAt.Equal(first.EvaluatedAt) {
		t.Fatal(second, err)
	}
	if first.Items[0].Resolution.RecordID == second.Items[0].Resolution.RecordID || second.Items[0].Resolution.RecordID == newRecord.RecordID {
		t.Fatal("pagination duplicated or included later write")
	}
	all := q
	all.Limit = 10
	page, err := engine.ListFacts(ctx, c, all)
	if err != nil || len(page.Items) != 3 {
		t.Fatal(page, err)
	}
	all.KnownAt = &marker
	page, err = engine.ListFacts(ctx, c, all)
	if err != nil || len(page.Items) != 2 || page.LedgerRevision != first.LedgerRevision {
		t.Fatal("known-at listing", page, err)
	}
	old := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	all.KnownAt = &old
	page, err = engine.ListFacts(ctx, c, all)
	if err != nil || len(page.Items) != 0 || page.LedgerRevision != 0 {
		t.Fatal("zero historical boundary became latest", page, err)
	}
	fact, err := engine.GetFact(ctx, c, model.FactQuery{Key: model.FactKey{Scope: "case", Subject: "one", Predicate: "color"}, ValidAt: from, KnownAt: &old, Purpose: "review"})
	if err != nil || fact.State != "missing" || fact.Value != nil {
		t.Fatal(fact, err)
	}
	wrong := next
	wrong.Scope = "other-scope"
	if _, err = engine.ListFacts(ctx, c, wrong); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("cursor scope binding", err)
	}
	other := c
	other.Tenant = "other-bank"
	if _, err = engine.ListFacts(ctx, other, next); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("cursor tenant binding", err)
	}
	denied, _ := facts.NewWithReads(l, denyPermission{permission: "can_read"}, store, options)
	if _, err = denied.ListFacts(ctx, c, next); !errors.Is(err, model.ErrDenied) {
		t.Fatal("revoked cursor access", err)
	}
	deniedHistory, _ := facts.NewWithReads(l, denyPermission{permission: "can_read_history"}, store, options)
	if _, err = deniedHistory.ListFacts(ctx, c, all); !errors.Is(err, model.ErrDenied) {
		t.Fatal("known-at history bypass", err)
	}
	if _, err = deniedHistory.GetFact(ctx, c, model.FactQuery{Key: model.FactKey{Scope: "case", Subject: "one", Predicate: "color"}, ValidAt: from, KnownAt: &old, Purpose: "review"}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("known-at fact history bypass", err)
	}
}
