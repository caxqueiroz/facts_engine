package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/model"
)

func testEvidenceDerivation(t *testing.T, ctx context.Context, l model.Ledger, store *evidence.Postgres) {
	c := model.Caller{Tenant: "derivative-bank", Principal: "author"}
	options := model.EngineOptions{RedactionPolicyVersions: []string{"approved-v1"}}
	e, err := facts.NewWithOptions(l, allow{}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	registerTestIdentities(t, ctx, e, c.Tenant, "case", []string{"subject"})
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "redaction-test"} }
	original, err := store.Put(ctx, c.Tenant, "original", []byte("I SECRET prefer tea."))
	if err != nil {
		t.Fatal(err)
	}
	source := model.Evidence{ID: "original", Scope: "case", SourceSystem: "chat", SourceObject: "message", SourceRevision: "v1", Speaker: "customer", SourceClass: "customer", SourceTime: time.Now().UTC(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "retain", Object: original}
	if _, err = e.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: cmd("original"), Evidence: source}); err != nil {
		t.Fatal(err)
	}
	object, err := store.Put(ctx, c.Tenant, "redacted", []byte("I prefer tea."))
	if err != nil {
		t.Fatal(err)
	}
	r := model.RedactedEvidenceRequest{Command: cmd("redact"), ID: "redacted", Scope: "case", ParentEvidenceID: "original", PolicyVersion: "approved-v1", Object: object, Ranges: []model.EvidenceCopyRange{{OriginalEnd: 2}, {OriginalStart: 9, OriginalEnd: 20, DerivedStart: 2}}}
	plain, _ := facts.New(l, allow{}, store)
	if _, err = plain.RegisterRedactedEvidence(ctx, c, r); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("unapproved redaction", err)
	}
	for _, permission := range []string{"can_propose", "can_read_evidence"} {
		denied, err := facts.NewWithOptions(l, denyPermission{permission: permission}, store, options)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = denied.RegisterRedactedEvidence(ctx, c, r); !errors.Is(err, model.ErrDenied) {
			t.Fatal("permission bypass", permission, err)
		}
	}
	bad := r
	bad.Ranges = []model.EvidenceCopyRange{{OriginalEnd: 13}}
	if _, err = e.RegisterRedactedEvidence(ctx, c, bad); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("false mapping", err)
	}
	result, err := e.RegisterRedactedEvidence(ctx, c, r)
	if err != nil {
		t.Fatal(err)
	}
	again, err := e.RegisterRedactedEvidence(ctx, c, r)
	if err != nil || again != result {
		t.Fatal("retry", again, result, err)
	}
	content, err := e.GetEvidenceContent(ctx, c, model.EvidenceQuery{ID: result.ID, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil || string(content.Bytes) != "I prefer tea." || content.Metadata.Classification != source.Classification || content.Metadata.SourceClass != source.SourceClass || content.Metadata.RetentionPolicy != source.RetentionPolicy || content.Metadata.Derivation == nil {
		t.Fatal("derivative provenance", content, err)
	}
	t.Run("remote evidence operations", func(t *testing.T) {
		metadataOnly, err := facts.New(l, denyPermission{permission: "can_read_evidence"}, store)
		if err != nil {
			t.Fatal(err)
		}
		testRemoteEvidence(t, ctx, e, metadataOnly, c, source, r)
	})
	if _, err = e.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: cmd("spoof"), Evidence: content.Metadata}); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("forged derivation", err)
	}
	chain := r
	chain.ID = "chain"
	chain.ParentEvidenceID = result.ID
	chain.Command = cmd("chain")
	if _, err = e.RegisterRedactedEvidence(ctx, c, chain); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("chain accepted", err)
	}
	status, err := e.GetRetentionStatus(ctx, c, model.RetentionStatusQuery{Scope: r.Scope, EvidenceID: source.ID, Purpose: r.Purpose})
	if err != nil || !slices.Contains(status.Blockers, "evidence_derivatives") {
		t.Fatal("parent not protected", status, err)
	}
	page, err := e.ListChanges(ctx, c, model.ChangesQuery{Scope: r.Scope, Purpose: r.Purpose, AfterRevision: result.LedgerRevision - 1, Limit: 1})
	if err != nil || len(page.Events) != 1 {
		t.Fatal(page, err)
	}
	archived, err := l.(model.ArchiveSource).ArchivePayload(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant}, Event: page.Events[0]})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{source.ID, result.ID} {
		if !slices.ContainsFunc(archived.Entries, func(entry model.AuditEntry) bool { return entry.Kind == "evidence" && entry.ID == id }) {
			t.Fatal("missing archive ancestry", id)
		}
	}
	if _, err = e.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{Scope: r.Scope, ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = e.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: cmd("schema"), Definition: model.PredicateVersion{Scope: r.Scope, Predicate: "preference", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	value := "tea"
	proposal := model.FactProposal{Command: cmd("proposal"), Key: model.FactKey{Scope: r.Scope, Subject: "subject", Predicate: "preference"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Sources: []model.SourceSpan{{EvidenceID: result.ID, Start: 0, End: 12, Quote: "I prefer tea"}}}
	if _, err = e.ProposeFact(ctx, c, proposal); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("cross-gap citation", err)
	}
	proposal.Sources = []model.SourceSpan{{EvidenceID: result.ID, Start: 9, End: 12, Quote: "tea"}}
	if _, err = e.ProposeFact(ctx, c, proposal); err != nil {
		t.Fatal("copied citation", err)
	}
	if _, err = e.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: cmd("memory-type"), Definition: model.MemoryType{Scope: r.Scope, Subtype: "episode", Version: "v1", PolicyID: "manual", Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	memory := model.MemoryProposal{Command: cmd("memory"), Key: model.MemoryKey{Scope: r.Scope, ID: "summary"}, Subtype: "episode", SchemaVersion: "v1", Body: "Tea preference", Sources: []model.SourceSpan{{EvidenceID: result.ID, Start: 0, End: 12, Quote: "I prefer tea"}}}
	if _, err = e.ProposeMemory(ctx, c, memory); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("memory cross-gap citation", err)
	}
	memory.Sources = proposal.Sources
	if _, err = e.ProposeMemory(ctx, c, memory); err != nil {
		t.Fatal("memory copied citation", err)
	}
	// Mapping remains part of the immutable evidence JSON, including archive data.
	raw, _ := json.Marshal(content.Metadata)
	var decoded model.Evidence
	if err = json.Unmarshal(raw, &decoded); err != nil || decoded.Derivation == nil {
		t.Fatal(err)
	}
	span, err := decoded.Derivation.OriginalSpan(proposal.Sources[0])
	if err != nil || span.Start != 16 || span.End != 19 || span.EvidenceID != source.ID {
		t.Fatal(span, err)
	}
}
