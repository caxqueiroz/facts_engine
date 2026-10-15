package postgres_test

import (
	"context"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testGRPCFactLifecycle(t *testing.T, ctx context.Context, ledger model.Ledger, store *evidence.Postgres) {
	t.Helper()
	e, err := facts.New(ledger, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	author := model.Caller{Tenant: "grpc-fact-lifecycle", Principal: "author"}
	reviewer := model.Caller{Tenant: author.Tenant, Principal: "reviewer"}
	registerTestIdentities(t, ctx, e, author.Tenant, "case", []string{"subject", "rejected"})
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "review"} }
	command := func(key string) *v1.Command { return &v1.Command{Key: key, Purpose: "review"} }
	if _, err := e.RegisterAcceptancePolicy(ctx, author, model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{Scope: "case", ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.RegisterPredicateVersion(ctx, author, model.PredicateRequest{Command: cmd("schema"), Definition: model.PredicateVersion{Scope: "case", Predicate: "preference", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	cut := at.Add(30 * time.Minute)
	object, err := store.Put(ctx, author.Tenant, "source", []byte("tea coffee"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.RegisterEvidence(ctx, author, model.EvidenceRequest{Command: cmd("source"), Evidence: model.Evidence{ID: "source", Scope: "case", SourceSystem: "test", SourceObject: "source", SourceRevision: "v1", Speaker: "customer", SourceClass: "customer", SourceTime: at, MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "retain", Object: object}}); err != nil {
		t.Fatal(err)
	}
	a := v1.NewFactServiceClient(governanceGRPC(t, e, author))
	r := v1.NewFactServiceClient(governanceGRPC(t, e, reviewer))
	connectReview := memoryv1connect.NewFactServiceClient(governanceClient(t, e, reviewer))
	key := model.FactKey{Scope: "case", Subject: "subject", Predicate: "preference"}
	proposal := &v1.ProposeFactRequest{Tenant: author.Tenant, Command: command("original"), Key: &v1.FactKey{Scope: key.Scope, Subject: key.Subject, Predicate: key.Predicate}, SchemaVersion: "v1", Value: &v1.Value{Payload: &v1.Value_Text{Text: "tea"}}, Valid: &v1.Interval{From: timestamppb.New(at)}, Sources: []*v1.SourceSpan{{EvidenceId: "source", Start: 0, End: 3, Quote: "tea"}}}
	original, err := a.ProposeFact(ctx, proposal)
	if err != nil {
		t.Fatal(err)
	}
	checkCommit := func(remote *v1.CommitResult, direct model.CommitResult, err error) {
		t.Helper()
		if err != nil || remote.Id != direct.ID || remote.RecordId != direct.RecordID || remote.VersionId != direct.VersionID || remote.ContentDigest != direct.ContentDigest || remote.StreamRevision != direct.StreamRevision || remote.LedgerRevision != direct.LedgerRevision || remote.RecordKind != direct.RecordKind {
			t.Fatal("embedded/gRPC receipt differs", remote, direct, err)
		}
	}
	checkRead := func(when time.Time, revision int64, state, value string) {
		t.Helper()
		query := &v1.GetFactRequest{Tenant: author.Tenant, Purpose: "review", Key: proposal.Key, ValidAt: timestamppb.New(when), KnowledgeRevision: revision}
		got, err := a.GetFact(ctx, query)
		if err != nil || got.Result.State != state || got.Result.Value.GetText() != value || (state != "resolved" && got.Result.Value != nil) {
			t.Fatal("gRPC temporal resolution", got, err)
		}
		viaConnect, err := connectReview.GetFact(ctx, query)
		if err != nil || !proto.Equal(got, viaConnect) {
			t.Fatal("Connect temporal resolution differs", viaConnect, err)
		}
		direct, err := e.GetFact(ctx, author, model.FactQuery{Key: key, Purpose: "review", ValidAt: when, KnowledgeRevision: revision})
		if err != nil || direct.State != got.Result.State || direct.VersionID != got.Result.VersionId || direct.LedgerRevision != got.Result.LedgerRevision {
			t.Fatal("embedded temporal resolution differs", direct, err)
		}
	}
	approval := &v1.ApproveFactRequest{Tenant: author.Tenant, Command: command("approve"), Scope: "case", RecordId: original.Result.RecordId, VersionId: original.Result.VersionId, ExpectedRevision: original.Result.StreamRevision}
	if result, err := a.ApproveFact(ctx, approval); result != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatal("gRPC self approval", result, err)
	}
	approved, err := r.ApproveFact(ctx, approval)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := e.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("approve"), Scope: "case", RecordID: original.Result.RecordId, VersionID: original.Result.VersionId, ExpectedRevision: original.Result.StreamRevision})
	checkCommit(approved.Result, direct, err)
	connectApproved, err := connectReview.ApproveFact(ctx, approval)
	if err != nil || !proto.Equal(approved, connectApproved) {
		t.Fatal("Connect approval replay", connectApproved, err)
	}
	checkRead(at, 0, "resolved", "tea")
	change := model.LifecycleChange{Approval: model.Approval{Command: cmd("dispute"), Scope: "case", RecordID: original.Result.RecordId, VersionID: original.Result.VersionId, ExpectedRevision: approved.Result.StreamRevision}, Reason: "review", Valid: model.Interval{From: &cut}}
	dispute := &v1.DisputeFactRequest{Tenant: author.Tenant, Command: command("dispute"), Scope: "case", RecordId: change.RecordID, VersionId: change.VersionID, ExpectedRevision: change.ExpectedRevision, Reason: change.Reason, Valid: &v1.Interval{From: timestamppb.New(cut)}}
	disputed, err := r.DisputeFact(ctx, dispute)
	if err != nil {
		t.Fatal(err)
	}
	direct, err = e.DisputeFact(ctx, reviewer, change)
	checkCommit(disputed.Result, direct, err)
	connectDisputed, err := connectReview.DisputeFact(ctx, dispute)
	if err != nil || !proto.Equal(disputed, connectDisputed) {
		t.Fatal("Connect dispute replay", connectDisputed, err)
	}
	checkRead(at, 0, "resolved", "tea")
	checkRead(cut, 0, "disputed", "")
	checkRead(cut, approved.Result.LedgerRevision, "resolved", "tea")
	change.Command = cmd("resolve")
	change.ExpectedRevision = disputed.Result.StreamRevision
	resolve := &v1.ResolveDisputeRequest{Tenant: author.Tenant, Command: command("resolve"), Scope: "case", RecordId: change.RecordID, VersionId: change.VersionID, ExpectedRevision: change.ExpectedRevision, Reason: change.Reason, Valid: dispute.Valid}
	resolved, err := r.ResolveDispute(ctx, resolve)
	if err != nil {
		t.Fatal(err)
	}
	direct, err = e.ResolveDispute(ctx, reviewer, change)
	checkCommit(resolved.Result, direct, err)
	connectResolved, err := connectReview.ResolveDispute(ctx, resolve)
	if err != nil || !proto.Equal(resolved, connectResolved) {
		t.Fatal("Connect resolution replay", connectResolved, err)
	}
	checkRead(cut, 0, "resolved", "tea")
	replacement := proto.Clone(proposal).(*v1.ProposeFactRequest)
	replacement.Command = command("replacement")
	replacement.ExpectedRevision = resolved.Result.StreamRevision
	replacement.Value = &v1.Value{Payload: &v1.Value_Text{Text: "coffee"}}
	replacement.Valid = dispute.Valid
	replacement.Sources = []*v1.SourceSpan{{EvidenceId: "source", Start: 4, End: 10, Quote: "coffee"}}
	next, err := a.ProposeFact(ctx, replacement)
	if err != nil {
		t.Fatal(err)
	}
	supersede := &v1.SupersedeFactRequest{Tenant: author.Tenant, Command: command("supersede"), Scope: "case", RecordId: change.RecordID, VersionId: next.Result.VersionId, ExpectedRevision: next.Result.StreamRevision, Reason: "correction", Valid: dispute.Valid, ReplacedVersionIds: []string{original.Result.VersionId}}
	superseded, err := r.SupersedeFact(ctx, supersede)
	if err != nil {
		t.Fatal(err)
	}
	change.Command = cmd("supersede")
	change.VersionID = next.Result.VersionId
	change.ExpectedRevision = next.Result.StreamRevision
	change.Reason = "correction"
	direct, err = e.SupersedeFact(ctx, reviewer, model.Supersession{Approval: change.Approval, Valid: change.Valid, Reason: change.Reason, ReplacedVersionIDs: []string{original.Result.VersionId}})
	checkCommit(superseded.Result, direct, err)
	connectSuperseded, err := connectReview.SupersedeFact(ctx, supersede)
	if err != nil || !proto.Equal(superseded, connectSuperseded) {
		t.Fatal("Connect supersession replay", connectSuperseded, err)
	}
	checkRead(at, 0, "resolved", "tea")
	checkRead(cut, 0, "resolved", "coffee")
	checkRead(cut, approved.Result.LedgerRevision, "resolved", "tea")
	change.Command = cmd("retract")
	change.ExpectedRevision = superseded.Result.StreamRevision
	change.Reason = "withdrawn"
	retract := &v1.RetractFactRequest{Tenant: author.Tenant, Command: command("retract"), Scope: "case", RecordId: change.RecordID, VersionId: change.VersionID, ExpectedRevision: change.ExpectedRevision, Reason: change.Reason, Valid: dispute.Valid}
	retracted, err := r.RetractFact(ctx, retract)
	if err != nil {
		t.Fatal(err)
	}
	direct, err = e.RetractFact(ctx, reviewer, change)
	checkCommit(retracted.Result, direct, err)
	connectRetracted, err := connectReview.RetractFact(ctx, retract)
	if err != nil || !proto.Equal(retracted, connectRetracted) {
		t.Fatal("Connect retraction replay", connectRetracted, err)
	}
	checkRead(cut, 0, "retracted", "")
	checkRead(cut, superseded.Result.LedgerRevision, "resolved", "coffee")
	checkRead(at, 0, "resolved", "tea")
	rejectedProposal := proto.Clone(proposal).(*v1.ProposeFactRequest)
	rejectedProposal.Command = command("rejected-proposal")
	rejectedProposal.Key.Subject = "rejected"
	candidate, err := a.ProposeFact(ctx, rejectedProposal)
	if err != nil {
		t.Fatal(err)
	}
	reject := &v1.RejectFactRequest{Tenant: author.Tenant, Command: command("reject"), Scope: "case", RecordId: candidate.Result.RecordId, VersionId: candidate.Result.VersionId, ExpectedRevision: candidate.Result.StreamRevision, Reason: "unsupported"}
	rejected, err := r.RejectFact(ctx, reject)
	if err != nil {
		t.Fatal(err)
	}
	direct, err = e.RejectFact(ctx, reviewer, model.LifecycleChange{Approval: model.Approval{Command: cmd("reject"), Scope: "case", RecordID: candidate.Result.RecordId, VersionID: candidate.Result.VersionId, ExpectedRevision: candidate.Result.StreamRevision}, Reason: "unsupported"})
	checkCommit(rejected.Result, direct, err)
	connectRejected, err := connectReview.RejectFact(ctx, reject)
	if err != nil || !proto.Equal(rejected, connectRejected) {
		t.Fatal("Connect rejection replay", connectRejected, err)
	}
}
