package postgres_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
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

func testRemoteMemories(t *testing.T, ctx context.Context, ledger model.Ledger, store *evidence.Postgres) {
	t.Helper()
	engine, err := facts.NewWithReads(ledger, allow{}, store, model.ReadOptions{PaginationKey: bytes.Repeat([]byte{17}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	author := model.Caller{Tenant: "remote-memory", Principal: "author"}
	reviewer := model.Caller{Tenant: author.Tenant, Principal: "reviewer"}
	acceptor := model.Caller{Tenant: author.Tenant, Principal: "acceptor", Service: true}
	registerTestIdentities(t, ctx, engine, author.Tenant, "case", []string{"subject"})
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "memory-review"} }
	_, err = engine.RegisterAcceptancePolicy(ctx, author, model.PolicyRequest{Command: cmd("policy"), Policy: model.AcceptancePolicy{Scope: "case", ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.RegisterPredicateVersion(ctx, author, model.PredicateRequest{Command: cmd("predicate"), Definition: model.PredicateVersion{Scope: "case", Predicate: "preference", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	object, err := store.Put(ctx, author.Tenant, "source", []byte("tea"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.RegisterEvidence(ctx, author, model.EvidenceRequest{Command: cmd("source"), Evidence: model.Evidence{ID: "source", Scope: "case", SourceSystem: "test", SourceObject: "source", SourceRevision: "1", Speaker: "customer", SourceClass: "customer", MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "test", SourceTime: at, Object: object}})
	if err != nil {
		t.Fatal(err)
	}
	value := "tea"
	fact, err := engine.ProposeFact(ctx, author, model.FactProposal{Command: cmd("fact"), Key: model.FactKey{Scope: "case", Subject: "subject", Predicate: "preference"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Valid: model.Interval{From: &at}, Sources: []model.SourceSpan{{EvidenceID: "source", Start: 0, End: 3, Quote: "tea"}}})
	if err != nil {
		t.Fatal(err)
	}
	acceptedFact, err := engine.ApproveFact(ctx, reviewer, model.Approval{Command: cmd("fact-approval"), Scope: "case", RecordID: fact.RecordID, VersionID: fact.VersionID, ExpectedRevision: fact.StreamRevision})
	if err != nil {
		t.Fatal(err)
	}
	authorRPC := governanceClient(t, engine, author)
	reviewerRPC := governanceClient(t, engine, reviewer)
	grpcAuthorConn := governanceGRPC(t, engine, author)
	grpcClient := v1.NewMemoryServiceClient(grpcAuthorConn)
	grpcReview := v1.NewMemoryServiceClient(governanceGRPC(t, engine, reviewer))
	definitions := memoryv1connect.NewDefinitionServiceClient(authorRPC)
	memoryType := &v1.RegisterMemoryTypeRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "memory-type", Purpose: "memory-review"}, Definition: &v1.MemoryType{Scope: "case", Subtype: "summary", Version: "v1", PolicyId: "manual", Classification: "confidential"}}
	published, err := definitions.RegisterMemoryType(ctx, memoryType)
	if err != nil {
		t.Fatal(err)
	}
	grpcType, err := v1.NewDefinitionServiceClient(grpcAuthorConn).RegisterMemoryType(ctx, memoryType)
	if err != nil || !proto.Equal(published, grpcType) {
		t.Fatal("gRPC memory type replay", grpcType, err)
	}
	typeRetry, err := engine.RegisterMemoryType(ctx, author, model.MemoryTypeRequest{Command: cmd("memory-type"), Definition: model.MemoryType{Scope: "case", Subtype: "summary", Version: "v1", PolicyID: "manual", Classification: "confidential"}})
	if err != nil || typeRetry.LedgerRevision != published.Result.LedgerRevision {
		t.Fatalf("memory type replay: %+v, %v", typeRetry, err)
	}
	client := memoryv1connect.NewMemoryServiceClient(authorRPC)
	review := memoryv1connect.NewMemoryServiceClient(reviewerRPC)
	automatic := memoryv1connect.NewMemoryServiceClient(governanceClient(t, engine, acceptor))
	request := &v1.ProposeMemoryRequest{Tenant: author.Tenant, Proposal: &v1.MemoryProposal{Command: &v1.Command{Key: "memory", Purpose: "memory-review"}, Key: &v1.MemoryKey{Scope: "case", Id: "summary"}, Subtype: "summary", SchemaVersion: "v1", Body: "Customer prefers tea.", Valid: &v1.Interval{From: timestamppb.New(at)}, Sources: []*v1.SourceSpan{{EvidenceId: "source", Start: 0, End: 3, Quote: "tea"}}, Dependencies: []*v1.MemoryDependency{{Scope: "case", RecordId: fact.RecordID, VersionId: fact.VersionID, ContentDigest: fact.ContentDigest, Label: "assertion"}}}}
	proposed, err := client.ProposeMemory(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	commit := proposed.Result
	embedded := model.MemoryProposal{Command: cmd("memory"), Key: model.MemoryKey{Scope: "case", ID: "summary"}, Subtype: "summary", SchemaVersion: "v1", Body: request.Proposal.Body, Valid: model.Interval{From: &at}, Sources: []model.SourceSpan{{EvidenceID: "source", Start: 0, End: 3, Quote: "tea"}}, Dependencies: []model.MemoryDependency{{Scope: "case", RecordID: fact.RecordID, VersionID: fact.VersionID, ContentDigest: fact.ContentDigest, Label: "assertion"}}}
	retry, err := engine.ProposeMemory(ctx, author, embedded)
	if err != nil || retry.RecordKind != "memory" || retry.ID != commit.Id || retry.LedgerRevision != commit.LedgerRevision || retry.ContentDigest != commit.ContentDigest {
		t.Fatalf("memory replay: %+v, %v", retry, err)
	}
	retained, err := client.GetMemoryVersion(ctx, &v1.GetMemoryVersionRequest{Tenant: author.Tenant, Scope: "case", RecordId: commit.RecordId, VersionId: commit.VersionId, Purpose: "memory-review"})
	if err != nil || retained.Version.Digest != commit.ContentDigest || retained.Version.Proposal.Body != embedded.Body || retained.Version.Proposal.Purpose != embedded.Purpose || !proto.Equal(retained.Version.Proposal.Key, request.Proposal.Key) || len(retained.Version.Proposal.Sources) != 1 || !proto.Equal(retained.Version.Proposal.Sources[0], request.Proposal.Sources[0]) || len(retained.Version.Proposal.Dependencies) != 1 || !proto.Equal(retained.Version.Proposal.Dependencies[0], request.Proposal.Dependencies[0]) || retained.Version.Proposer.Principal != author.Principal || len(retained.Version.Authorization) != 2 {
		t.Fatalf("version provenance: %v, %v", retained, err)
	}
	grpcRetained, err := grpcClient.GetMemoryVersion(ctx, &v1.GetMemoryVersionRequest{Tenant: author.Tenant, Scope: "case", RecordId: commit.RecordId, VersionId: commit.VersionId, Purpose: "memory-review"})
	if err != nil || !proto.Equal(retained, grpcRetained) {
		t.Fatal("gRPC memory version provenance", grpcRetained, err)
	}
	approval := &v1.ApproveMemoryRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "approve", Purpose: "memory-review"}, Scope: "case", RecordId: commit.RecordId, VersionId: commit.VersionId, ExpectedRevision: commit.StreamRevision}
	if _, err := client.ApproveMemory(ctx, approval); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("self approval: %v", err)
	}
	if _, err := automatic.AcceptMemory(ctx, &v1.AcceptMemoryRequest{Tenant: author.Tenant, Command: approval.Command, Scope: approval.Scope, RecordId: approval.RecordId, VersionId: approval.VersionId, ExpectedRevision: approval.ExpectedRevision}); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("manual policy accepted automatically: %v", err)
	}
	approved, err := grpcReview.ApproveMemory(ctx, approval)
	if err != nil {
		t.Fatal(err)
	}
	approvedReplay, err := review.ApproveMemory(ctx, approval)
	if err != nil || !proto.Equal(approved, approvedReplay) {
		t.Fatal("gRPC/Connect manual memory approval", approvedReplay, err)
	}
	query := &v1.GetMemoryRequest{Tenant: author.Tenant, Purpose: "memory-review", Key: request.Proposal.Key, ValidAt: request.Proposal.Valid.From}
	resolved, err := client.GetMemory(ctx, query)
	if err != nil || resolved.Result.State != "resolved" || resolved.Result.Body != embedded.Body || len(resolved.Result.Dependencies) != 1 {
		t.Fatalf("memory read: %v, %v", resolved, err)
	}
	grpcresolved, err := grpcClient.GetMemory(ctx, query)
	if err != nil || !proto.Equal(resolved, grpcresolved) {
		t.Fatal("gRPC memory read", grpcresolved, err)
	}
	disputedRequest := &v1.DisputeMemoryRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "dispute", Purpose: "memory-review"}, Scope: "case", RecordId: commit.RecordId, VersionId: commit.VersionId, ExpectedRevision: approved.Result.StreamRevision, Reason: "review_needed", Valid: request.Proposal.Valid}
	disputed, err := grpcReview.DisputeMemory(ctx, disputedRequest)
	if err != nil {
		t.Fatal(err)
	}
	disputedReplay, err := review.DisputeMemory(ctx, disputedRequest)
	if err != nil || !proto.Equal(disputed, disputedReplay) {
		t.Fatal("gRPC/Connect DisputeMemory replay", disputedReplay, err)
	}
	hidden, err := client.GetMemory(ctx, query)
	if err != nil || hidden.Result.State != "disputed" || hidden.Result.Body != "" {
		t.Fatalf("dispute disclosure: %v, %v", hidden, err)
	}
	grpchidden, err := grpcClient.GetMemory(ctx, query)
	if err != nil || !proto.Equal(hidden, grpchidden) {
		t.Fatal("gRPC dispute disclosure", grpchidden, err)
	}
	restoredRequest := &v1.ResolveMemoryDisputeRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "resolve", Purpose: "memory-review"}, Scope: "case", RecordId: commit.RecordId, VersionId: commit.VersionId, ExpectedRevision: disputed.Result.StreamRevision, Reason: "review_complete", Valid: request.Proposal.Valid}
	restored, err := grpcReview.ResolveMemoryDispute(ctx, restoredRequest)
	if err != nil {
		t.Fatal(err)
	}
	restoredReplay, err := review.ResolveMemoryDispute(ctx, restoredRequest)
	if err != nil || !proto.Equal(restored, restoredReplay) {
		t.Fatal("gRPC/Connect ResolveMemoryDispute replay", restoredReplay, err)
	}
	replacement := proto.Clone(request).(*v1.ProposeMemoryRequest)
	replacement.Proposal.Command.Key = "replacement"
	replacement.Proposal.Body = "Reviewed preference for tea."
	replacement.Proposal.ExpectedRevision = restored.Result.StreamRevision
	next, err := client.ProposeMemory(ctx, replacement)
	if err != nil {
		t.Fatal(err)
	}
	supersededRequest := &v1.SupersedeMemoryRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "supersede", Purpose: "memory-review"}, Scope: "case", RecordId: commit.RecordId, VersionId: next.Result.VersionId, ExpectedRevision: next.Result.StreamRevision, Reason: "correction", Valid: request.Proposal.Valid, ReplacedVersionIds: []string{commit.VersionId}}
	superseded, err := grpcReview.SupersedeMemory(ctx, supersededRequest)
	if err != nil {
		t.Fatal(err)
	}
	supersededReplay, err := review.SupersedeMemory(ctx, supersededRequest)
	if err != nil || !proto.Equal(superseded, supersededReplay) {
		t.Fatal("gRPC/Connect SupersedeMemory replay", supersededReplay, err)
	}
	after, err := client.GetMemory(ctx, query)
	if err != nil || after.Result.Body != replacement.Proposal.Body || after.Result.VersionId != next.Result.VersionId {
		t.Fatalf("replacement: %v, %v", after, err)
	}
	grpcafter, err := grpcClient.GetMemory(ctx, query)
	if err != nil || !proto.Equal(after, grpcafter) {
		t.Fatal("gRPC replacement", grpcafter, err)
	}
	// A fact dependency change must suppress the current body without rewriting history.
	factChange := model.LifecycleChange{Approval: model.Approval{Command: cmd("fact-dispute"), Scope: "case", RecordID: fact.RecordID, VersionID: fact.VersionID, ExpectedRevision: acceptedFact.StreamRevision}, Valid: model.Interval{From: &at}, Reason: "review"}
	changed, err := engine.DisputeFact(ctx, reviewer, factChange)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := client.GetMemory(ctx, query)
	if err != nil || stale.Result.State != "stale" || stale.Result.Body != "" {
		t.Fatalf("dependency invalidation: %v, %v", stale, err)
	}
	grpcstale, err := grpcClient.GetMemory(ctx, query)
	if err != nil || !proto.Equal(stale, grpcstale) {
		t.Fatal("gRPC dependency invalidation", grpcstale, err)
	}
	historical := proto.Clone(query).(*v1.GetMemoryRequest)
	historical.KnowledgeRevision = superseded.Result.LedgerRevision
	old, err := client.GetMemory(ctx, historical)
	if err != nil || old.Result.Body != replacement.Proposal.Body {
		t.Fatalf("historical memory rewritten: %v, %v", old, err)
	}
	grpcold, err := grpcClient.GetMemory(ctx, historical)
	if err != nil || !proto.Equal(old, grpcold) {
		t.Fatal("gRPC historical memory rewritten", grpcold, err)
	}
	factChange.Command = cmd("fact-resolve")
	factChange.ExpectedRevision = changed.StreamRevision
	if _, err := engine.ResolveDispute(ctx, reviewer, factChange); err != nil {
		t.Fatal(err)
	}
	retractedRequest := &v1.RetractMemoryRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "retract", Purpose: "memory-review"}, Scope: "case", RecordId: commit.RecordId, VersionId: next.Result.VersionId, ExpectedRevision: superseded.Result.StreamRevision, Reason: "withdrawn", Valid: request.Proposal.Valid}
	retracted, err := grpcReview.RetractMemory(ctx, retractedRequest)
	if err != nil {
		t.Fatal(err)
	}
	retractedReplay, err := review.RetractMemory(ctx, retractedRequest)
	if err != nil || !proto.Equal(retracted, retractedReplay) {
		t.Fatal("gRPC/Connect RetractMemory replay", retractedReplay, err)
	}
	current, err := client.GetMemory(ctx, query)
	if err != nil || current.Result.State != "retracted" || current.Result.Body != "" {
		t.Fatalf("retracted memory: %v, %v", current, err)
	}
	grpccurrent, err := grpcClient.GetMemory(ctx, query)
	if err != nil || !proto.Equal(current, grpccurrent) {
		t.Fatal("gRPC retracted memory", grpccurrent, err)
	}
	historyRequest := &v1.GetMemoryHistoryRequest{Tenant: author.Tenant, Scope: "case", RecordId: commit.RecordId, Purpose: "memory-review", Limit: 2}
	history, err := grpcClient.GetMemoryHistory(ctx, historyRequest)
	if err != nil || len(history.Events) != 2 || history.NextPageToken == "" || history.StreamRevision != retracted.Result.StreamRevision || history.Events[0].RecordKind != "memory" {
		t.Fatalf("memory history: %v, %v", history, err)
	}
	historyRequest.PageToken = history.NextPageToken
	page, err := client.GetMemoryHistory(ctx, historyRequest)
	if err != nil || len(page.Events) != 2 || page.Events[0].StreamRevision != 3 || page.LedgerRevision != history.LedgerRevision {
		t.Fatalf("history continuation: %v, %v", page, err)
	}
	grpcPage, err := grpcClient.GetMemoryHistory(ctx, historyRequest)
	if err != nil || !proto.Equal(page, grpcPage) {
		t.Fatal("gRPC/Connect memory history continuation", grpcPage, err)
	}
	if _, err := review.GetMemoryHistory(ctx, historyRequest); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("history cursor actor: %v", err)
	}
	if result, err := grpcReview.GetMemoryHistory(ctx, historyRequest); result != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal("gRPC memory history actor binding", result, err)
	}
	if _, err := memoryv1connect.NewFactServiceClient(authorRPC).GetFactHistory(ctx, &v1.GetFactHistoryRequest{Tenant: author.Tenant, Scope: "case", RecordId: commit.RecordId, Purpose: "memory-review", Limit: 2}); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("memory exposed as fact: %v", err)
	}
	if result, err := v1.NewFactServiceClient(grpcAuthorConn).GetFactHistory(ctx, &v1.GetFactHistoryRequest{Tenant: author.Tenant, Scope: "case", RecordId: commit.RecordId, Purpose: "memory-review", Limit: 2}); result != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatal("gRPC memory exposed as fact", result, err)
	}
	rejectedRequest := proto.Clone(request).(*v1.ProposeMemoryRequest)
	rejectedRequest.Proposal.Command.Key = "reject-candidate"
	rejectedRequest.Proposal.Key.Id = "reject-candidate"
	candidate, err := client.ProposeMemory(ctx, rejectedRequest)
	if err != nil {
		t.Fatal(err)
	}
	rejection := &v1.RejectMemoryRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "reject", Purpose: "memory-review"}, Scope: "case", RecordId: candidate.Result.RecordId, VersionId: candidate.Result.VersionId, ExpectedRevision: candidate.Result.StreamRevision, Reason: "unsupported"}
	rejected, err := grpcReview.RejectMemory(ctx, rejection)
	if err != nil {
		t.Fatal(err)
	}
	rejectedReplay, err := review.RejectMemory(ctx, rejection)
	if err != nil || !proto.Equal(rejected, rejectedReplay) {
		t.Fatal("gRPC/Connect memory rejection", rejectedReplay, err)
	}
	// Automated memory acceptance remains a distinct policy-controlled operation.
	_, err = definitions.RegisterAcceptancePolicy(ctx, &v1.RegisterAcceptancePolicyRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "auto-policy", Purpose: "memory-review"}, Policy: &v1.AcceptancePolicy{Scope: "case", Id: "automatic", Mode: "automated", AcceptingServices: []string{acceptor.Principal}, SourceClasses: []string{"customer"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = definitions.RegisterMemoryType(ctx, &v1.RegisterMemoryTypeRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "auto-type", Purpose: "memory-review"}, Definition: &v1.MemoryType{Scope: "case", Subtype: "automatic", Version: "v1", PolicyId: "automatic", Classification: "confidential"}})
	if err != nil {
		t.Fatal(err)
	}
	automaticRequest := proto.Clone(request).(*v1.ProposeMemoryRequest)
	automaticRequest.Proposal.Command.Key = "auto-candidate"
	automaticRequest.Proposal.Key.Id = "automatic"
	automaticRequest.Proposal.Subtype = "automatic"
	autoCandidate, err := client.ProposeMemory(ctx, automaticRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := review.ApproveMemory(ctx, &v1.ApproveMemoryRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "wrong-mode", Purpose: "memory-review"}, Scope: "case", RecordId: autoCandidate.Result.RecordId, VersionId: autoCandidate.Result.VersionId, ExpectedRevision: autoCandidate.Result.StreamRevision}); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("automated policy approved manually: %v", err)
	}
	_, err = automatic.AcceptMemory(ctx, &v1.AcceptMemoryRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "auto-accept", Purpose: "memory-review"}, Scope: "case", RecordId: autoCandidate.Result.RecordId, VersionId: autoCandidate.Result.VersionId, ExpectedRevision: autoCandidate.Result.StreamRevision})
	if err != nil {
		t.Fatal(err)
	}
	autoRead, err := client.GetMemory(ctx, &v1.GetMemoryRequest{Tenant: author.Tenant, Purpose: "memory-review", Key: automaticRequest.Proposal.Key, ValidAt: request.Proposal.Valid.From})
	if err != nil || autoRead.Result.State != "resolved" || autoRead.Result.Body != automaticRequest.Proposal.Body {
		t.Fatalf("automatic memory: %v, %v", autoRead, err)
	}

}
