package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/model"
)

func testMemoryListing(t *testing.T, ctx context.Context, l model.Ledger, store *evidence.Postgres) {
	t.Helper()
	options := model.ReadOptions{PaginationKey: bytes.Repeat([]byte{23}, 32)}
	engine, err := facts.NewWithReads(l, allow{}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	author := model.Caller{Tenant: "memory-listing-bank", Principal: "author"}
	reviewer := model.Caller{Tenant: author.Tenant, Principal: "reviewer"}
	registerTestIdentities(t, ctx, engine, author.Tenant, "case", nil)
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "memory-listing"} }
	if _, err = engine.RegisterAcceptancePolicy(ctx, author, model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{Scope: "case", ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
		t.Fatal(err)
	}
	for _, subtype := range []string{"summary", "episode"} {
		if _, err = engine.RegisterMemoryType(ctx, author, model.MemoryTypeRequest{Command: cmd("type-" + subtype), Definition: model.MemoryType{Scope: "case", Subtype: subtype, Version: "v1", PolicyID: "manual", Classification: "confidential"}}); err != nil {
			t.Fatal(err)
		}
	}
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	object, err := store.Put(ctx, author.Tenant, "source", []byte("tea"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.RegisterEvidence(ctx, author, model.EvidenceRequest{Command: cmd("source"), Evidence: model.Evidence{ID: "source", Scope: "case", SourceSystem: "test", SourceObject: "source", SourceRevision: "1", Speaker: "customer", SourceClass: "customer", MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "test", SourceTime: from, Object: object}}); err != nil {
		t.Fatal(err)
	}
	propose := func(id, subtype string) model.CommitResult {
		t.Helper()
		r, err := engine.ProposeMemory(ctx, author, model.MemoryProposal{Command: cmd("propose-" + id), Key: model.MemoryKey{Scope: "case", ID: id}, Subtype: subtype, SchemaVersion: "v1", Body: "Memory " + id + ": customer prefers tea.", Valid: model.Interval{From: &from}, Sources: []model.SourceSpan{{EvidenceID: "source", Start: 0, End: 3, Quote: "tea"}}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	approve := func(r model.CommitResult) model.CommitResult {
		t.Helper()
		approved, err := engine.ApproveMemory(ctx, reviewer, model.Approval{Command: cmd("approve-" + r.VersionID), Scope: "case", RecordID: r.RecordID, VersionID: r.VersionID, ExpectedRevision: r.StreamRevision})
		if err != nil {
			t.Fatal(err)
		}
		return approved
	}
	approve(propose("alpha", "summary"))
	approve(propose("beta", "summary"))
	approve(propose("gamma", "episode"))
	propose("pending", "summary")
	rejected := propose("rejected", "summary")
	if _, err = engine.RejectMemory(ctx, reviewer, model.LifecycleChange{Approval: model.Approval{Command: cmd("reject"), Scope: "case", RecordID: rejected.RecordID, VersionID: rejected.VersionID, ExpectedRevision: rejected.StreamRevision}, Reason: "unsupported"}); err != nil {
		t.Fatal(err)
	}
	retracted := approve(propose("retracted", "summary"))
	if _, err = engine.RetractMemory(ctx, reviewer, model.LifecycleChange{Approval: model.Approval{Command: cmd("retract"), Scope: "case", RecordID: retracted.RecordID, VersionID: retracted.VersionID, ExpectedRevision: retracted.StreamRevision}, Valid: model.Interval{From: &from}, Reason: "withdrawn"}); err != nil {
		t.Fatal(err)
	}
	if got, err := engine.GetMemory(ctx, author, model.MemoryQuery{Key: model.MemoryKey{Scope: "case", ID: "retracted"}, Purpose: "consolidate", ValidAt: from}); err != nil || got.State != "retracted" {
		t.Fatal("retraction fixture", got, err)
	}
	ids := func(items []model.MemoryListItem) []string {
		out := []string{}
		for _, item := range items {
			out = append(out, item.Key.ID)
		}
		slices.Sort(out)
		return out
	}

	q := model.MemoriesQuery{Scope: "case", Purpose: "consolidate", ValidAt: from, Limit: 100}
	all, err := engine.ListMemories(ctx, author, q)
	if err != nil || all.NextPageToken != "" || all.LedgerRevision == 0 || !slices.Equal(ids(all.Items), []string{"alpha", "beta", "gamma"}) {
		t.Fatal("rejected, retracted or pending memory listed", all, err)
	}
	for _, item := range all.Items {
		single, err := engine.GetMemory(ctx, author, model.MemoryQuery{Key: item.Key, Purpose: q.Purpose, ValidAt: from})
		if err != nil || item.Memory.State != "resolved" || !reflect.DeepEqual(item.Memory, single) {
			t.Fatalf("listed memory differs from GetMemory: %+v, %+v, %v", item, single, err)
		}
	}
	subtype := q
	subtype.Subtype = "episode"
	episodes, err := engine.ListMemories(ctx, author, subtype)
	if err != nil || !slices.Equal(ids(episodes.Items), []string{"gamma"}) || episodes.Items[0].Memory.Body == "" {
		t.Fatal("subtype filter", episodes, err)
	}

	// Pages may be short when skipped records fall inside them; the cursor still advances.
	paged := q
	paged.Limit = 1
	first, err := engine.ListMemories(ctx, author, paged)
	if err != nil || first.NextPageToken == "" {
		t.Fatal("first page", first, err)
	}
	pinned := first.LedgerRevision
	approve(propose("delta", "summary"))
	collected := append([]model.MemoryListItem{}, first.Items...)
	next := paged
	next.PageToken = first.NextPageToken
	pages := 1
	for next.PageToken != "" {
		page, err := engine.ListMemories(ctx, author, next)
		if err != nil || len(page.Items) > 1 || page.LedgerRevision != pinned || !page.EvaluatedAt.Equal(first.EvaluatedAt) {
			t.Fatal("continuation page", page, err)
		}
		collected = append(collected, page.Items...)
		next.PageToken = page.NextPageToken
		pages++
		if pages > 20 {
			t.Fatal("pagination did not terminate")
		}
	}
	if pages < 3 || !slices.Equal(ids(collected), []string{"alpha", "beta", "gamma"}) {
		t.Fatal("paging duplicated, dropped or included a later memory", pages, ids(collected))
	}
	latest, err := engine.ListMemories(ctx, author, q)
	if err != nil || !slices.Equal(ids(latest.Items), []string{"alpha", "beta", "delta", "gamma"}) || latest.LedgerRevision <= pinned {
		t.Fatal("fresh listing", latest, err)
	}

	historical := q
	historical.KnowledgeRevision = pinned
	before, err := engine.ListMemories(ctx, author, historical)
	if err != nil || before.LedgerRevision != pinned || !slices.Equal(ids(before.Items), []string{"alpha", "beta", "gamma"}) {
		t.Fatal("historical listing", before, err)
	}
	old := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	empty := q
	empty.KnownAt = &old
	if page, err := engine.ListMemories(ctx, author, empty); err != nil || len(page.Items) != 0 || page.LedgerRevision != 0 || page.NextPageToken != "" {
		t.Fatal("zero historical boundary became latest", page, err)
	}

	// A continuation token is bound to the caller and the full query.
	cursor := paged
	cursor.PageToken = first.NextPageToken
	for name, change := range map[string]func(*model.MemoriesQuery){
		"scope":    func(q *model.MemoriesQuery) { q.Scope = "other-scope" },
		"subtype":  func(q *model.MemoriesQuery) { q.Subtype = "summary" },
		"purpose":  func(q *model.MemoriesQuery) { q.Purpose = "other" },
		"valid at": func(q *model.MemoriesQuery) { q.ValidAt = from.Add(time.Hour) },
		"limit":    func(q *model.MemoriesQuery) { q.Limit = 2 },
	} {
		changed := cursor
		change(&changed)
		if _, err = engine.ListMemories(ctx, author, changed); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("cursor replayed with a different query:", name, err)
		}
	}
	if _, err = engine.ListMemories(ctx, reviewer, cursor); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("cursor replayed by another principal", err)
	}
	other := author
	other.Tenant = "other-bank"
	if _, err = engine.ListMemories(ctx, other, cursor); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("cursor replayed by another tenant", err)
	}
	if _, err = engine.ListFacts(ctx, author, model.FactsQuery{Scope: "case", Purpose: q.Purpose, ValidAt: from, Limit: 1, PageToken: first.NextPageToken}); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("memory cursor accepted by ListFacts", err)
	}

	denied, _ := facts.NewWithReads(l, denyPermission{permission: "can_read"}, store, options)
	if _, err = denied.ListMemories(ctx, author, q); !errors.Is(err, model.ErrDenied) {
		t.Fatal("unauthorized listing", err)
	}
	if _, err = denied.ListMemories(ctx, author, cursor); !errors.Is(err, model.ErrDenied) {
		t.Fatal("revoked cursor access", err)
	}
	deniedHistory, _ := facts.NewWithReads(l, denyPermission{permission: "can_read_history"}, store, options)
	if _, err = deniedHistory.ListMemories(ctx, author, historical); !errors.Is(err, model.ErrDenied) {
		t.Fatal("knowledge revision history bypass", err)
	}
	if _, err = deniedHistory.ListMemories(ctx, author, empty); !errors.Is(err, model.ErrDenied) {
		t.Fatal("known-at history bypass", err)
	}
	if page, err := deniedHistory.ListMemories(ctx, author, q); err != nil || len(page.Items) != 4 {
		t.Fatal("current listing needs only can_read", page, err)
	}
	unkeyed, _ := facts.New(l, allow{}, store)
	if _, err = unkeyed.ListMemories(ctx, author, q); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("listing without a pagination key", err)
	}
}
