package postgres_test

import (
	"context"
	"errors"
	"fmt"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"slices"
	"testing"
	"time"
)

func testConfirmationArtifacts(t *testing.T, ctx context.Context, e *facts.Engine, store *evidence.Postgres, c model.Caller, scope, prefix string, version model.CommitResult) *model.ClientConfirmation {
	t.Helper()
	at := time.Now().UTC().Truncate(time.Microsecond)
	register := func(role, body string) model.ArtifactReference {
		id := prefix + "-" + role
		ref, err := store.Put(ctx, c.Tenant, id, []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		_, err = e.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: model.Command{Key: id, Purpose: "confirmation"}, Evidence: model.Evidence{ID: id, Scope: scope, SourceSystem: "confirmation-ui", SourceObject: id, SourceRevision: "v1", Speaker: c.Principal, SourceClass: "customer", SourceTime: at, MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "test", Object: ref}})
		if err != nil {
			t.Fatal(err)
		}
		return model.ArtifactReference{EvidenceID: id, Digest: ref.Digest}
	}
	return &model.ClientConfirmation{Presentation: register("presentation", fmt.Sprintf("Confirm version %s with digest %s: tea", version.VersionID, version.ContentDigest)), Response: register("response", "I confirm the displayed information."), PresentationVersion: "test-ui-v1", PresentedAt: at, RespondedAt: at}
}

type corruptConfirmationRead struct {
	model.EvidenceStore
	digest string
}

func (s corruptConfirmationRead) Read(ctx context.Context, tenant string, ref model.ObjectRef) ([]byte, error) {
	if ref.Digest == s.digest {
		return []byte("tampered"), nil
	}
	return s.EvidenceStore.Read(ctx, tenant, ref)
}
func testConfirmations(t *testing.T, ctx context.Context, l *postgres.Ledger, store *evidence.Postgres) {
	e, _ := facts.New(l, allow{}, store)
	c := model.Caller{Tenant: "confirmation-bank", Principal: "client"}
	svc := model.Caller{Tenant: c.Tenant, Principal: "acceptor", Service: true}
	registerTestIdentities(t, ctx, e, c.Tenant, "case", []string{"customer"})
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "confirmation-test"} }
	_, err := e.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{Scope: "case", ID: "confirmed", Mode: "automated", SourceClasses: []string{"customer"}, AcceptingServices: []string{"acceptor"}, Required: []model.AttestationRequirement{{Kind: "client_confirmation", Principals: []string{c.Principal}}}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: cmd("schema"), Definition: model.PredicateVersion{Scope: "case", Predicate: "preference", Version: "v1", PolicyID: "confirmed", Kind: model.Text, Classification: "confidential"}})
	if err != nil {
		t.Fatal(err)
	}
	source := testConfirmationArtifacts(t, ctx, e, store, c, "case", "source", model.CommitResult{})
	from := time.Now().UTC().Truncate(time.Microsecond)
	value := "I confirm"
	factRequest := model.FactProposal{Command: cmd("fact"), Key: model.FactKey{Scope: "case", Subject: "customer", Predicate: "preference"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &from}, Sources: []model.SourceSpan{{EvidenceID: source.Response.EvidenceID, Start: 0, End: 9, Quote: value}}}
	fact, err := e.ProposeFact(ctx, c, factRequest)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: cmd("memory-type"), Definition: model.MemoryType{Scope: "case", Subtype: "summary", Version: "v1", PolicyID: "confirmed", Classification: "confidential"}})
	if err != nil {
		t.Fatal(err)
	}
	memoryRequest := model.MemoryProposal{Command: cmd("memory"), Key: model.MemoryKey{Scope: "case", ID: "summary"}, Subtype: "summary", SchemaVersion: "v1", Body: "Client confirmed information.", Valid: factRequest.Valid, Sources: factRequest.Sources}
	memory, err := e.ProposeMemory(ctx, c, memoryRequest)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []model.CommitResult{fact, memory} {
		kind := "fact"
		if v.RecordKind == "memory" {
			kind = "memory"
		}
		confirmation := testConfirmationArtifacts(t, ctx, e, store, c, "case", kind, v)
		att := model.AttestationRequest{Command: cmd(kind + "-confirm"), Attestation: model.Attestation{ID: kind + "-confirmation", Scope: "case", RecordID: v.RecordID, VersionID: v.VersionID, ContentDigest: v.ContentDigest, Kind: "client_confirmation", EvidenceID: confirmation.Response.EvidenceID, Attestor: c, Confirmation: confirmation}}
		missing := att
		missing.Attestation.Confirmation = nil
		if _, err = e.RecordAttestation(ctx, c, missing); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("missing presentation", err)
		}
		bad := att
		changed := *confirmation
		changed.Presentation.Digest = model.BytesDigest([]byte("wrong"))
		bad.Attestation.Confirmation = &changed
		if _, err = e.RecordAttestation(ctx, c, bad); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("artifact digest", err)
		}
		denied, _ := facts.New(l, denyPermission{permission: "can_read_evidence"}, store)
		if _, err = denied.RecordAttestation(ctx, c, att); !errors.Is(err, model.ErrDenied) {
			t.Fatal("artifact disclosure permission", err)
		}
		recorded, err := e.RecordAttestation(ctx, c, att)
		if err != nil {
			t.Fatal(err)
		}
		again, err := e.RecordAttestation(ctx, c, att)
		if err != nil || recorded != again {
			t.Fatal("confirmation retry", again, err)
		}
		replay := att
		replay.Command = cmd(kind + "-replayed-response")
		replay.Attestation.ID = kind + "-replayed-confirmation"
		if _, err = e.RecordAttestation(ctx, c, replay); !errors.Is(err, model.ErrConflict) {
			t.Fatal("response replay", err)
		}
		// Relabeling the same immutable object as another evidence ID cannot
		// manufacture a fresh client response.
		responseMetadata, err := e.GetEvidenceMetadata(ctx, c, model.EvidenceQuery{ID: confirmation.Response.EvidenceID, Scope: "case", Purpose: "review"})
		if err != nil {
			t.Fatal(err)
		}
		responseMetadata.ID = kind + "-response-alias"
		responseMetadata.SourceObject = responseMetadata.ID
		if _, err = e.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: cmd(responseMetadata.ID), Evidence: responseMetadata}); err != nil {
			t.Fatal(err)
		}
		replay.Command = cmd(kind + "-alias-replay")
		alias := *confirmation
		alias.Response.EvidenceID = responseMetadata.ID
		replay.Attestation.Confirmation = &alias
		replay.Attestation.EvidenceID = responseMetadata.ID
		if _, err = e.RecordAttestation(ctx, c, replay); !errors.Is(err, model.ErrConflict) {
			t.Fatal("response alias replay", err)
		}
		status, err := e.GetRetentionStatus(ctx, c, model.RetentionStatusQuery{Scope: "case", EvidenceID: confirmation.Presentation.EvidenceID, Purpose: "review"})
		if err != nil || !slices.Contains(status.Blockers, "confirmation_artifacts") {
			t.Fatal("presentation disposal guard", status, err)
		}
		corrupt, _ := facts.New(l, allow{}, corruptConfirmationRead{store, confirmation.Presentation.Digest})
		approval := model.Approval{Command: cmd(kind + "-accept"), Scope: "case", RecordID: v.RecordID, VersionID: v.VersionID, ExpectedRevision: v.StreamRevision}
		accept := e.AcceptFact
		rejectCorrupt := corrupt.AcceptFact
		if kind == "memory" {
			accept = e.AcceptMemory
			rejectCorrupt = corrupt.AcceptMemory
		}
		if _, err = rejectCorrupt(ctx, svc, approval); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("unverified confirmation accepted", err)
		}
		if _, err = accept(ctx, svc, approval); err != nil {
			t.Fatal(err)
		}
		if kind == "fact" {
			got, err := corrupt.GetFact(ctx, c, model.FactQuery{Key: factRequest.Key, ValidAt: from, Purpose: "review"})
			if err != nil || got.State != "ineligible" {
				t.Fatal("corrupt confirmation read", got, err)
			}
		} else {
			got, err := corrupt.GetMemory(ctx, c, model.MemoryQuery{Key: memoryRequest.Key, ValidAt: from, Purpose: "review"})
			if err != nil || got.State != "ineligible" {
				t.Fatal("corrupt memory confirmation", got, err)
			}
		}
		page, err := e.ListChanges(ctx, c, model.ChangesQuery{Scope: "case", Purpose: "archive", AfterRevision: recorded.LedgerRevision - 1, Limit: 1})
		if err != nil || len(page.Events) != 1 {
			t.Fatal(page, err)
		}
		archived, err := l.ArchivePayload(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant}, Event: page.Events[0]})
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range model.AttestationEvidence(att.Attestation) {
			if !slices.ContainsFunc(archived.Entries, func(entry model.AuditEntry) bool { return entry.Kind == "evidence" && entry.ID == id }) {
				t.Fatal("archive omitted confirmation evidence", id)
			}
		}
	}
}
