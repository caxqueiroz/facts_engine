package postgres_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func governanceGRPC(t *testing.T, engine *facts.Engine, caller model.Caller) *grpc.ClientConn {
	t.Helper()
	handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return caller, nil }, connectapi.Limits{Timeout: 5 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	conn, err := grpc.NewClient(strings.TrimPrefix(server.URL, "https://"), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func testGRPCMemoryGovernance(t *testing.T, ctx context.Context, ledger model.Ledger, store *evidence.Postgres) {
	t.Helper()
	engine, err := facts.New(ledger, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	author := model.Caller{Tenant: "grpc-memory-governance", Principal: "author"}
	reviewer := model.Caller{Tenant: author.Tenant, Principal: "reviewer"}
	acceptor := model.Caller{Tenant: author.Tenant, Principal: "acceptor", Service: true}
	registerTestIdentities(t, ctx, engine, author.Tenant, "case", nil)
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "memory-review"} }
	rpcCmd := func(key string) *v1.Command { return &v1.Command{Key: key, Purpose: "memory-review"} }
	policy := model.AcceptancePolicy{ID: "reviewed", Scope: "case", Mode: "automated", AcceptingServices: []string{acceptor.Principal}, SourceClasses: []string{"customer"}, Required: []model.AttestationRequirement{{Kind: "human_review", Principals: []string{reviewer.Principal, author.Principal}, RequireExpiry: true, MaxAgeSeconds: 3600}}}
	if _, err := engine.RegisterAcceptancePolicy(ctx, author, model.PolicyRequest{Command: cmd("policy"), Policy: policy}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RegisterMemoryType(ctx, author, model.MemoryTypeRequest{Command: cmd("type"), Definition: model.MemoryType{Scope: "case", Subtype: "summary", Version: "v1", PolicyID: policy.ID, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	ref, err := store.Put(ctx, author.Tenant, "source", []byte("tea"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RegisterEvidence(ctx, author, model.EvidenceRequest{Command: cmd("source"), Evidence: model.Evidence{ID: "source", Scope: "case", SourceSystem: "test", SourceObject: "source", SourceRevision: "v1", Speaker: "customer", SourceClass: "customer", SourceTime: at, MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "retain", Object: ref}}); err != nil {
		t.Fatal(err)
	}
	authorConn := governanceGRPC(t, engine, author)
	reviewerConn := governanceGRPC(t, engine, reviewer)
	accepting := v1.NewMemoryServiceClient(governanceGRPC(t, engine, acceptor))
	memories := v1.NewMemoryServiceClient(authorConn)
	reviews := v1.NewAttestationServiceClient(reviewerConn)
	proposal := model.MemoryProposal{Command: cmd("propose"), Key: model.MemoryKey{Scope: "case", ID: "summary"}, Subtype: "summary", SchemaVersion: "v1", Body: "Customer prefers tea.", Valid: model.Interval{From: &at}, Sources: []model.SourceSpan{{EvidenceID: "source", Start: 0, End: 3, Quote: "tea"}}}
	proposed, err := memories.ProposeMemory(ctx, &v1.ProposeMemoryRequest{Tenant: author.Tenant, Proposal: &v1.MemoryProposal{Command: rpcCmd("propose"), Key: &v1.MemoryKey{Scope: "case", Id: "summary"}, Subtype: proposal.Subtype, SchemaVersion: proposal.SchemaVersion, Body: proposal.Body, Valid: &v1.Interval{From: timestamppb.New(at)}, Sources: []*v1.SourceSpan{{EvidenceId: "source", Start: 0, End: 3, Quote: "tea"}}}})
	if err != nil {
		t.Fatal(err)
	}
	original, err := engine.ProposeMemory(ctx, author, proposal)
	assertCommit := func(remote *v1.CommitResult, local model.CommitResult) {
		t.Helper()
		if remote.Id != local.ID || remote.RecordId != local.RecordID || remote.VersionId != local.VersionID || remote.ContentDigest != local.ContentDigest || remote.StreamRevision != local.StreamRevision || remote.LedgerRevision != local.LedgerRevision {
			t.Fatal("receipt parity", remote, local)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	assertCommit(proposed.Result, original)
	approval := model.Approval{Command: cmd("accept"), Scope: "case", RecordID: original.RecordID, VersionID: original.VersionID, ExpectedRevision: original.StreamRevision}
	acceptance := &v1.AcceptMemoryRequest{Tenant: author.Tenant, Command: rpcCmd("accept"), Scope: "case", RecordId: original.RecordID, VersionId: original.VersionID, ExpectedRevision: original.StreamRevision}
	requireFailure := func(remote, local error, code codes.Code, want error) {
		t.Helper()
		if status.Code(remote) != code || !errors.Is(local, want) {
			t.Fatal("error parity", status.Code(remote), remote, local)
		}
	}
	_, remoteErr := accepting.AcceptMemory(ctx, acceptance)
	_, localErr := engine.AcceptMemory(ctx, acceptor, approval)
	requireFailure(remoteErr, localErr, codes.FailedPrecondition, model.ErrPrecondition)
	_, remoteErr = v1.NewMemoryServiceClient(reviewerConn).ApproveMemory(ctx, &v1.ApproveMemoryRequest{Tenant: author.Tenant, Command: rpcCmd("wrong-mode"), Scope: "case", RecordId: original.RecordID, VersionId: original.VersionID, ExpectedRevision: original.StreamRevision})
	manual := approval
	manual.Command = cmd("wrong-mode")
	_, localErr = engine.ApproveMemory(ctx, reviewer, manual)
	requireFailure(remoteErr, localErr, codes.FailedPrecondition, model.ErrPrecondition)
	expiry := at.Add(time.Hour)
	att := model.AttestationRequest{Command: cmd("review"), Attestation: model.Attestation{ID: "review", Scope: "case", RecordID: original.RecordID, VersionID: original.VersionID, ContentDigest: original.ContentDigest, Kind: "human_review", EvidenceID: "source", Attestor: reviewer, ExpiresAt: &expiry}}
	wireAtt := func(a model.AttestationRequest) *v1.RecordAttestationRequest {
		wire := &v1.AttestationInput{Id: a.Attestation.ID, Scope: a.Attestation.Scope, RecordId: a.Attestation.RecordID, VersionId: a.Attestation.VersionID, ContentDigest: a.Attestation.ContentDigest, Kind: a.Attestation.Kind, EvidenceId: a.Attestation.EvidenceID}
		if a.Attestation.ExpiresAt != nil {
			wire.ExpiresAt = timestamppb.New(*a.Attestation.ExpiresAt)
		}
		return &v1.RecordAttestationRequest{Tenant: author.Tenant, Command: rpcCmd(a.Key), Attestation: wire}
	}
	wrong := att
	wrong.Attestation.ContentDigest = model.BytesDigest([]byte("different-version"))
	_, remoteErr = reviews.RecordAttestation(ctx, wireAtt(wrong))
	_, localErr = engine.RecordAttestation(ctx, reviewer, wrong)
	requireFailure(remoteErr, localErr, codes.FailedPrecondition, model.ErrPrecondition)
	self := att
	self.Attestation.Attestor = author
	_, remoteErr = v1.NewAttestationServiceClient(authorConn).RecordAttestation(ctx, wireAtt(self))
	_, localErr = engine.RecordAttestation(ctx, author, self)
	requireFailure(remoteErr, localErr, codes.FailedPrecondition, model.ErrPrecondition)
	past := at.Add(-time.Minute)
	wrong = att
	wrong.Attestation.ExpiresAt = &past
	_, remoteErr = reviews.RecordAttestation(ctx, wireAtt(wrong))
	_, localErr = engine.RecordAttestation(ctx, reviewer, wrong)
	requireFailure(remoteErr, localErr, codes.InvalidArgument, model.ErrInvalid)
	wrong = att
	wrong.Attestation.ExpiresAt = nil
	_, remoteErr = reviews.RecordAttestation(ctx, wireAtt(wrong))
	_, localErr = engine.RecordAttestation(ctx, reviewer, wrong)
	requireFailure(remoteErr, localErr, codes.FailedPrecondition, model.ErrPrecondition)
	recorded, err := reviews.RecordAttestation(ctx, wireAtt(att))
	if err != nil {
		t.Fatal(err)
	}
	retained, err := engine.RecordAttestation(ctx, reviewer, att)
	if err != nil {
		t.Fatal(err)
	}
	assertCommit(recorded.Result, retained)
	for _, caller := range []model.Caller{{Tenant: author.Tenant, Principal: acceptor.Principal}, {Tenant: author.Tenant, Principal: "other-service", Service: true}} {
		_, remoteErr = v1.NewMemoryServiceClient(governanceGRPC(t, engine, caller)).AcceptMemory(ctx, acceptance)
		_, localErr = engine.AcceptMemory(ctx, caller, approval)
		requireFailure(remoteErr, localErr, codes.FailedPrecondition, model.ErrPrecondition)
	}
	accepted, err := accepting.AcceptMemory(ctx, acceptance)
	if err != nil {
		t.Fatal(err)
	}
	acceptedLocal, err := engine.AcceptMemory(ctx, acceptor, approval)
	if err != nil {
		t.Fatal(err)
	}
	assertCommit(accepted.Result, acceptedLocal)
	assertRead := func(revision int64, wantState, wantVersion string) {
		t.Helper()
		remote, err := memories.GetMemory(ctx, &v1.GetMemoryRequest{Tenant: author.Tenant, Purpose: "memory-review", Key: &v1.MemoryKey{Scope: "case", Id: "summary"}, ValidAt: timestamppb.New(at), KnowledgeRevision: revision})
		local, localErr := engine.GetMemory(ctx, author, model.MemoryQuery{Key: proposal.Key, Purpose: "memory-review", ValidAt: at, KnowledgeRevision: revision})
		if err != nil || localErr != nil || remote.Result.State != wantState || local.State != wantState || remote.Result.VersionId != wantVersion || local.VersionID != wantVersion || remote.Result.Body != local.Body || remote.Result.LedgerRevision != local.LedgerRevision {
			t.Fatal("read parity", remote, local, err, localErr)
		}
	}
	assertRead(0, "resolved", original.VersionID)
	withdrawal := model.Withdrawal{Command: cmd("withdraw"), Scope: "case", AttestationID: att.Attestation.ID}
	wireWithdrawal := &v1.WithdrawAttestationRequest{Tenant: author.Tenant, Command: rpcCmd("withdraw"), Scope: "case", AttestationId: att.Attestation.ID}
	_, remoteErr = v1.NewAttestationServiceClient(authorConn).WithdrawAttestation(ctx, wireWithdrawal)
	_, localErr = engine.WithdrawAttestation(ctx, author, withdrawal)
	requireFailure(remoteErr, localErr, codes.PermissionDenied, model.ErrDenied)
	withdrawn, err := reviews.WithdrawAttestation(ctx, wireWithdrawal)
	if err != nil {
		t.Fatal(err)
	}
	withdrawnLocal, err := engine.WithdrawAttestation(ctx, reviewer, withdrawal)
	if err != nil {
		t.Fatal(err)
	}
	assertCommit(withdrawn.Result, withdrawnLocal)
	assertRead(0, "ineligible", original.VersionID)
	assertRead(acceptedLocal.LedgerRevision, "resolved", original.VersionID)
	// A replacement must obtain its own exact-digest review; the prior version's
	// approval and attestation do not authorize it.
	replacement := proposal
	replacement.Command = cmd("replacement")
	replacement.ExpectedRevision = withdrawnLocal.StreamRevision
	replacement.Body = "Updated tea summary."
	next, err := engine.ProposeMemory(ctx, author, replacement)
	if err != nil {
		t.Fatal(err)
	}
	stale := att
	stale.Command = cmd("replacement-review")
	stale.Attestation.ID = "replacement-review"
	stale.Attestation.VersionID = next.VersionID
	_, remoteErr = reviews.RecordAttestation(ctx, wireAtt(stale))
	_, localErr = engine.RecordAttestation(ctx, reviewer, stale)
	requireFailure(remoteErr, localErr, codes.FailedPrecondition, model.ErrPrecondition)
	supersede := &v1.SupersedeMemoryRequest{Tenant: author.Tenant, Command: rpcCmd("supersede"), Scope: "case", RecordId: next.RecordID, VersionId: next.VersionID, ExpectedRevision: next.StreamRevision, Valid: &v1.Interval{From: timestamppb.New(at)}, ReplacedVersionIds: []string{original.VersionID}, Reason: "reviewed_correction", Automated: true}
	supersession := model.Supersession{Approval: model.Approval{Command: cmd("supersede"), Scope: "case", RecordID: next.RecordID, VersionID: next.VersionID, ExpectedRevision: next.StreamRevision}, Valid: proposal.Valid, ReplacedVersionIDs: []string{original.VersionID}, Reason: "reviewed_correction", Automated: true}
	_, remoteErr = accepting.SupersedeMemory(ctx, supersede)
	_, localErr = engine.SupersedeMemory(ctx, acceptor, supersession)
	requireFailure(remoteErr, localErr, codes.FailedPrecondition, model.ErrPrecondition)
	stale.Attestation.ContentDigest = next.ContentDigest
	if _, err := reviews.RecordAttestation(ctx, wireAtt(stale)); err != nil {
		t.Fatal(err)
	}
	replaced, err := accepting.SupersedeMemory(ctx, supersede)
	if err != nil {
		t.Fatal(err)
	}
	replacedLocal, err := engine.SupersedeMemory(ctx, acceptor, supersession)
	if err != nil {
		t.Fatal(err)
	}
	assertCommit(replaced.Result, replacedLocal)
	assertRead(0, "resolved", next.VersionID)
	assertRead(acceptedLocal.LedgerRevision, "resolved", original.VersionID)
}
