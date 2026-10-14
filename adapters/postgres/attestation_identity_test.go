package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
	"time"
)

type identityVerifierFunc func(context.Context, model.AttestationIdentityRequest) (model.AttestationIdentityReceipt, error)

func (f identityVerifierFunc) VerifyAttestationIdentity(ctx context.Context, r model.AttestationIdentityRequest) (model.AttestationIdentityReceipt, error) {
	return f(ctx, r)
}

type denyIdentityCaller struct{ principal, permission string }

func (d denyIdentityCaller) Check(ctx context.Context, r model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	v, _ := (allow{}).Check(ctx, r)
	v.Allowed = r.Caller.Principal != d.principal || r.Permission != d.permission
	return v, nil
}
func testDelegatedAttestations(t *testing.T, ctx context.Context, l *postgres.Ledger, store *evidence.Postgres, admin *pgxpool.Pool) {
	base, _ := facts.New(l, allow{}, store)
	client := model.Caller{Tenant: "delegation-bank", Principal: "client"}
	recorder := model.Caller{Tenant: client.Tenant, Principal: "confirmation-service", Service: true}
	registerTestIdentities(t, ctx, base, client.Tenant, "case", []string{"customer"})
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "client-confirmation"} }
	_, err := base.RegisterAcceptancePolicy(ctx, client, model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{Scope: "case", ID: "policy", Mode: "automated", SourceClasses: []string{"customer"}, AcceptingServices: []string{"acceptor"}, Required: []model.AttestationRequirement{{Kind: "client_confirmation", Principals: []string{client.Principal}}}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = base.RegisterPredicateVersion(ctx, client, model.PredicateRequest{Command: cmd("schema"), Definition: model.PredicateVersion{Scope: "case", Predicate: "choice", Version: "v1", PolicyID: "policy", Kind: model.Text, Classification: "confidential"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = base.RegisterMemoryType(ctx, client, model.MemoryTypeRequest{Command: cmd("memory-type"), Definition: model.MemoryType{Scope: "case", Subtype: "summary", Version: "v1", PolicyID: "policy", Classification: "confidential"}})
	if err != nil {
		t.Fatal(err)
	}
	source := testConfirmationArtifacts(t, ctx, base, store, client, "case", "source", model.CommitResult{})
	at := time.Now().UTC().Truncate(time.Microsecond)
	value := "I confirm"
	proposal := model.FactProposal{Command: cmd("fact"), Key: model.FactKey{Scope: "case", Subject: "customer", Predicate: "choice"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &at}, Sources: []model.SourceSpan{{EvidenceID: source.Response.EvidenceID, Start: 0, End: 9, Quote: value}}}
	fact, err := base.ProposeFact(ctx, client, proposal)
	if err != nil {
		t.Fatal(err)
	}
	memoryRequest := model.MemoryProposal{Command: cmd("memory"), Key: model.MemoryKey{Scope: "case", ID: "summary"}, Subtype: "summary", SchemaVersion: "v1", Body: "Confirmed information", Valid: proposal.Valid, Sources: proposal.Sources}
	memory, err := base.ProposeMemory(ctx, client, memoryRequest)
	if err != nil {
		t.Fatal(err)
	}
	approved := map[string]string{}
	calls := 0
	verifier := identityVerifierFunc(func(ctx context.Context, r model.AttestationIdentityRequest) (model.AttestationIdentityReceipt, error) {
		calls++
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("unbounded verifier call")
		}
		digest := model.AttestationIdentityDigest(r)
		if approved[r.Request.GrantID] != digest {
			return model.AttestationIdentityReceipt{}, model.ErrDenied
		}
		now := time.Now().UTC()
		return model.AttestationIdentityReceipt{Authority: "trusted-idp", BindingID: r.Request.GrantID, RequestDigest: digest, Attestor: client, AuthenticatedAt: now.Add(-time.Minute), VerifiedAt: now, ExpiresAt: now.Add(time.Minute)}, nil
	})
	options := model.EngineOptions{AttestationIdentity: model.AttestationIdentityOptions{Verifier: verifier, Authority: "trusted-idp"}}
	e, err := facts.NewWithOptions(l, allow{}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []model.CommitResult{fact, memory} {
		kind := "fact"
		if v.RecordKind == "memory" {
			kind = "memory"
		}
		artifacts := testConfirmationArtifacts(t, ctx, base, store, client, "case", kind, v)
		request := model.DelegatedAttestationRequest{AttestationRequest: model.AttestationRequest{Command: cmd(kind + "-record"), Attestation: model.Attestation{ID: kind + "-confirmation", Scope: "case", RecordID: v.RecordID, VersionID: v.VersionID, ContentDigest: v.ContentDigest, Kind: "client_confirmation", EvidenceID: artifacts.Response.EvidenceID, Attestor: client, Confirmation: artifacts}}, GrantID: kind + "-grant"}
		approved[request.GrantID] = model.AttestationIdentityDigest(model.AttestationIdentityRequest{Recorder: recorder, Request: request})
		if _, err = base.RecordDelegatedAttestation(ctx, recorder, request); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("implicit verifier", err)
		}
		if _, err = e.RecordAttestation(ctx, recorder, request.AttestationRequest); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("direct API delegated bypass", err)
		}
		forged := request
		forged.Attestation.Attestor.Principal = "someone-else"
		if _, err = e.RecordDelegatedAttestation(ctx, recorder, forged); !errors.Is(err, model.ErrDenied) {
			t.Fatal("grant subject mismatch", err)
		}
		for _, denial := range []denyIdentityCaller{{recorder.Principal, "can_record_delegated_attestation"}, {client.Principal, "can_attest"}, {client.Principal, "can_read_evidence"}} {
			denied, _ := facts.NewWithOptions(l, denial, store, options)
			before := calls
			if _, err = denied.RecordDelegatedAttestation(ctx, recorder, request); !errors.Is(err, model.ErrDenied) || calls != before {
				t.Fatal("authorization before identity provider", err)
			}
		}
		if kind == "fact" {
			lock, err := admin.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Rollback(context.WithoutCancel(ctx))
			if _, err = lock.Exec(ctx, `SELECT revision FROM tenant_revisions WHERE tenant_id=$1 FOR UPDATE`, client.Tenant); err != nil {
				t.Fatal(err)
			}
			verified := make(chan struct{})
			expiring := options
			expiring.AttestationIdentity.Verifier = identityVerifierFunc(func(ctx context.Context, r model.AttestationIdentityRequest) (model.AttestationIdentityReceipt, error) {
				proof, err := verifier.VerifyAttestationIdentity(ctx, r)
				proof.ExpiresAt = time.Now().Add(200 * time.Millisecond)
				close(verified)
				return proof, err
			})
			waiting, _ := facts.NewWithOptions(l, allow{}, store, expiring)
			done := make(chan error, 1)
			go func() { _, err := waiting.RecordDelegatedAttestation(ctx, recorder, request); done <- err }()
			<-verified
			time.Sleep(300 * time.Millisecond)
			if err = lock.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err = <-done; !errors.Is(err, model.ErrUnavailable) {
				t.Fatal("expired grant committed after lock wait", err)
			}
			var consumed bool
			if err = admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM attestation_delegations WHERE tenant_id=$1 AND binding_id=$2)`, client.Tenant, request.GrantID).Scan(&consumed); err != nil || consumed {
				t.Fatal("failed verification consumed binding", consumed, err)
			}
		}
		committed, err := e.RecordDelegatedAttestation(ctx, recorder, request)
		if err != nil {
			t.Fatal(err)
		}
		again, err := e.RecordDelegatedAttestation(ctx, recorder, request)
		if err != nil || again != committed {
			t.Fatal("delegated retry", again, err)
		}
		// Binding IDs are single-use even if a verifier erroneously approves changed
		// input. Fresh artifacts prevent response replay protection from hiding this check.
		replay := request
		replay.Key = kind + "-replay"
		replay.Attestation.ID = kind + "-replayed-attestation"
		replay.Attestation.Confirmation = testConfirmationArtifacts(t, ctx, base, store, client, "case", kind+"-replay", v)
		replay.Attestation.EvidenceID = replay.Attestation.Confirmation.Response.EvidenceID
		approved[replay.GrantID] = model.AttestationIdentityDigest(model.AttestationIdentityRequest{Recorder: recorder, Request: replay})
		if _, err = e.RecordDelegatedAttestation(ctx, recorder, replay); !errors.Is(err, model.ErrConflict) {
			t.Fatal("identity grant replay", err)
		}
		approved[request.GrantID] = model.AttestationIdentityDigest(model.AttestationIdentityRequest{Recorder: recorder, Request: request})
		page, err := e.ListChanges(ctx, client, model.ChangesQuery{Scope: "case", Purpose: "audit", AfterRevision: committed.LedgerRevision - 1, Limit: 1})
		if err != nil || len(page.Events) != 1 {
			t.Fatal(page, err)
		}
		archive, err := l.ArchivePayload(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: client.Tenant}, Event: page.Events[0]})
		if err != nil {
			t.Fatal(err)
		}
		retained := false
		for _, entry := range archive.Entries {
			if entry.Kind == "attestation" || entry.Kind == "memory_attestation" {
				var a model.Attestation
				if err = json.Unmarshal(entry.Payload, &a); err != nil {
					t.Fatal(err)
				}
				if a.Recorder == nil || *a.Recorder != recorder || a.Attestor != client || a.Delegation == nil || len(a.Delegation.Authorization) != 5 || a.Delegation.Receipt.BindingID != request.GrantID {
					t.Fatal("identity lineage", a)
				}
				retained = true
			}
		}
		if !retained {
			t.Fatal("missing delegated attestation archive")
		}
		approval := model.Approval{Command: cmd(kind + "-accept"), Scope: "case", RecordID: v.RecordID, VersionID: v.VersionID, ExpectedRevision: v.StreamRevision}
		accept := e.AcceptFact
		if kind == "memory" {
			accept = e.AcceptMemory
		}
		if _, err = accept(ctx, model.Caller{Tenant: client.Tenant, Principal: "acceptor", Service: true}, approval); err != nil {
			t.Fatal("client policy binding", err)
		}
		withdrawal := model.Withdrawal{Command: cmd(kind + "-withdraw"), Scope: "case", AttestationID: request.Attestation.ID}
		spoof := client
		spoof.Service = true
		if _, err = e.WithdrawAttestation(ctx, spoof, withdrawal); !errors.Is(err, model.ErrDenied) {
			t.Fatal("principal kind confusion", err)
		}
		if _, err = e.WithdrawAttestation(ctx, recorder, withdrawal); !errors.Is(err, model.ErrDenied) {
			t.Fatal("recorder acquired client authority", err)
		}
		if _, err = e.WithdrawAttestation(ctx, client, withdrawal); err != nil {
			t.Fatal("client withdrawal", err)
		}
	}
}
