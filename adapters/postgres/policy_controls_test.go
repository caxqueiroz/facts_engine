package postgres_test

import (
	"context"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
	"time"
)

func testPolicyControls(t *testing.T, ctx context.Context, l *postgres.Ledger, store *evidence.Postgres, admin *pgxpool.Pool) {
	e, _ := facts.New(l, allow{}, store)
	proposer := model.Caller{Tenant: "policy-controls", Principal: "proposer"}
	reviewer := model.Caller{Tenant: proposer.Tenant, Principal: "reviewer"}
	acceptor := model.Caller{Tenant: proposer.Tenant, Principal: "acceptor", Service: true}
	registerTestIdentities(t, ctx, e, proposer.Tenant, "case", []string{"customer"})
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "policy-review"} }
	p := model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{Scope: "case", ID: "reviewed", Mode: "automated", AcceptingServices: []string{acceptor.Principal}, SourceClasses: []string{"customer"}, Required: []model.AttestationRequirement{{Kind: "human_review", Principals: []string{proposer.Principal, reviewer.Principal}, RequireExpiry: true, MaxAgeSeconds: 2}}}}
	if _, err := e.RegisterAcceptancePolicy(ctx, proposer, p); err != nil {
		t.Fatal(err)
	}
	manual := model.PolicyRequest{Command: cmd("default-manual"), Policy: model.AcceptancePolicy{Scope: "case", ID: "manual", SourceClasses: []string{"customer"}}}
	original, err := e.RegisterAcceptancePolicy(ctx, proposer, manual)
	if err != nil {
		t.Fatal(err)
	}
	manual.Policy.Mode = "manual"
	if again, err := e.RegisterAcceptancePolicy(ctx, proposer, manual); err != nil || again != original {
		t.Fatal("default manual canonicalization", again, err)
	}
	if _, err = e.RegisterPredicateVersion(ctx, proposer, model.PredicateRequest{Command: cmd("schema"), Definition: model.PredicateVersion{Scope: "case", Predicate: "choice", Version: "v1", PolicyID: "reviewed", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = e.RegisterMemoryType(ctx, proposer, model.MemoryTypeRequest{Command: cmd("memory-type"), Definition: model.MemoryType{Scope: "case", Subtype: "summary", Version: "v1", PolicyID: "reviewed", Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	source := testConfirmationArtifacts(t, ctx, e, store, proposer, "case", "source", model.CommitResult{})
	at := time.Now().UTC().Truncate(time.Microsecond)
	value := "I confirm"
	proposal := model.FactProposal{Command: cmd("fact"), Key: model.FactKey{Scope: "case", Subject: "customer", Predicate: "choice"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &at}, Sources: []model.SourceSpan{{EvidenceID: source.Response.EvidenceID, Start: 0, End: 9, Quote: value}}}
	fact, err := e.ProposeFact(ctx, proposer, proposal)
	if err != nil {
		t.Fatal(err)
	}
	memoryRequest := model.MemoryProposal{Command: cmd("memory"), Key: model.MemoryKey{Scope: "case", ID: "summary"}, Subtype: "summary", SchemaVersion: "v1", Body: "Information reviewed", Valid: proposal.Valid, Sources: proposal.Sources}
	memory, err := e.ProposeMemory(ctx, proposer, memoryRequest)
	if err != nil {
		t.Fatal(err)
	}
	commits := map[string]model.CommitResult{}
	for _, v := range []model.CommitResult{fact, memory} {
		kind := "fact"
		accept := e.AcceptFact
		if v.RecordKind == "memory" {
			kind = "memory"
			accept = e.AcceptMemory
		}
		approval := model.Approval{Command: cmd(kind + "-accept"), Scope: "case", RecordID: v.RecordID, VersionID: v.VersionID, ExpectedRevision: v.StreamRevision}
		if _, err = accept(ctx, acceptor, approval); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("automated acceptance skipped review", err)
		}
		expires := time.Now().Add(time.Hour)
		review := model.AttestationRequest{Command: cmd(kind + "-review"), Attestation: model.Attestation{ID: kind + "-review", Scope: "case", RecordID: v.RecordID, VersionID: v.VersionID, ContentDigest: v.ContentDigest, Kind: "human_review", EvidenceID: source.Presentation.EvidenceID, Attestor: reviewer, ExpiresAt: &expires}}
		own := review
		own.Attestation.Attestor = proposer
		if _, err = e.RecordAttestation(ctx, proposer, own); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("self-review allowed", err)
		}
		noExpiry := review
		noExpiry.Attestation.ExpiresAt = nil
		if _, err = e.RecordAttestation(ctx, reviewer, noExpiry); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("expiry requirement bypassed", err)
		}
		denied, _ := facts.New(l, denyPermission{permission: "can_approve"}, store)
		if _, err = denied.RecordAttestation(ctx, reviewer, review); !errors.Is(err, model.ErrDenied) {
			t.Fatal("review permission bypassed", err)
		}
		if _, err = e.RecordAttestation(ctx, reviewer, review); err != nil {
			t.Fatal(err)
		}
		if kind == "fact" {
			lock, err := admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Rollback(context.WithoutCancel(ctx))
			if _, err = lock.Exec(ctx, `SELECT revision FROM tenant_revisions WHERE tenant_id=$1 FOR UPDATE`, proposer.Tenant); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := accept(ctx, acceptor, approval); done <- err }()
			time.Sleep(2100 * time.Millisecond)
			if err = lock.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err = <-done; !errors.Is(err, model.ErrPrecondition) {
				t.Fatal("expired review committed after lock wait", err)
			}
			review.Command = cmd("renewed-fact-review")
			review.Attestation.ID = "renewed-fact-review"
			if _, err = e.RecordAttestation(ctx, reviewer, review); err != nil {
				t.Fatal(err)
			}
		}
		if commits[kind], err = accept(ctx, acceptor, approval); err != nil {
			t.Fatal("reviewed acceptance", err)
		}
	}
	// Expiry is policy-driven: the explicit attestation expiry is still an hour away.
	time.Sleep(2100 * time.Millisecond)
	current, err := e.GetFact(ctx, proposer, model.FactQuery{Key: proposal.Key, ValidAt: at, Purpose: "review"})
	if err != nil || current.State != "ineligible" {
		t.Fatal("fact age policy", current, err)
	}
	currentMemory, err := e.GetMemory(ctx, proposer, model.MemoryQuery{Key: memoryRequest.Key, ValidAt: at, Purpose: "review"})
	if err != nil || currentMemory.State != "ineligible" {
		t.Fatal("memory age policy", currentMemory, err)
	}
	historical, err := e.GetFact(ctx, proposer, model.FactQuery{Key: proposal.Key, ValidAt: at, KnowledgeRevision: commits["fact"].LedgerRevision, Purpose: "review"})
	if err != nil || historical.State != "resolved" {
		t.Fatal("fact historical policy time", historical, err)
	}
	historicalMemory, err := e.GetMemory(ctx, proposer, model.MemoryQuery{Key: memoryRequest.Key, ValidAt: at, KnowledgeRevision: commits["memory"].LedgerRevision, Purpose: "review"})
	if err != nil || historicalMemory.State != "resolved" {
		t.Fatal("memory historical policy time", historicalMemory, err)
	}
	// A replacement has its own immutable identity and cannot inherit the old review.
	proposal.Command = cmd("replacement")
	proposal.ExpectedRevision = commits["fact"].StreamRevision
	replacement, err := e.ProposeFact(ctx, proposer, proposal)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.SupersedeFact(ctx, acceptor, model.Supersession{Approval: model.Approval{Command: cmd("replace"), Scope: "case", RecordID: replacement.RecordID, VersionID: replacement.VersionID, ExpectedRevision: replacement.StreamRevision}, Automated: true, ReplacedVersionIDs: []string{fact.VersionID}, Valid: proposal.Valid, Reason: "fresh-review-needed"})
	if !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("replacement inherited review", err)
	}
}
