package postgres_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func governanceClient(t *testing.T, engine *facts.Engine, caller model.Caller) *connect.Client {
	t.Helper()
	h, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return caller, nil }, connectapi.Limits{Timeout: 5 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	return connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL))
}
func testRemoteGovernance(t *testing.T, ctx context.Context, ledger model.Ledger, store *evidence.Postgres) {
	t.Helper()
	engine, err := facts.New(ledger, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	author := model.Caller{Tenant: "remote-governance", Principal: "author"}
	reviewer := model.Caller{Tenant: author.Tenant, Principal: "reviewer"}
	acceptor := model.Caller{Tenant: author.Tenant, Principal: "acceptor", Service: true}
	registerTestIdentities(t, ctx, engine, author.Tenant, "case", []string{"subject"})
	authorRPC := governanceClient(t, engine, author)
	authorGRPC := governanceGRPC(t, engine, author)
	grpcDefinitions := v1.NewDefinitionServiceClient(authorGRPC)
	grpcFacts := v1.NewFactServiceClient(authorGRPC)
	grpcOwn := v1.NewAttestationServiceClient(authorGRPC)
	grpcReview := v1.NewAttestationServiceClient(governanceGRPC(t, engine, reviewer))
	grpcAccept := v1.NewFactServiceClient(governanceGRPC(t, engine, acceptor))
	definitions := memoryv1connect.NewDefinitionServiceClient(authorRPC)
	policy := &v1.RegisterAcceptancePolicyRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "policy", Purpose: "review"}, Policy: &v1.AcceptancePolicy{Scope: "case", Id: "policy", Mode: "automated", SourceClasses: []string{"customer"}, AcceptingServices: []string{acceptor.Principal}, Required: []*v1.AttestationRequirement{{Kind: "human_review", Principals: []string{reviewer.Principal, author.Principal}, RequireExpiry: true, MaxAgeSeconds: 3600}}}}
	denied, err := facts.New(ledger, denyPermission{permission: "can_manage_definitions"}, store)
	if err != nil {
		t.Fatal(err)
	}
	restricted := memoryv1connect.NewDefinitionServiceClient(governanceClient(t, denied, author))
	grpcRestricted := v1.NewDefinitionServiceClient(governanceGRPC(t, denied, author))
	if _, err := restricted.RegisterAcceptancePolicy(ctx, policy); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("definition authorization: %v", err)
	}
	if result, err := grpcRestricted.RegisterAcceptancePolicy(ctx, policy); result != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatal("gRPC definition authorization", result, err)
	}
	published, err := definitions.RegisterAcceptancePolicy(ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	grpcPolicy, err := grpcDefinitions.RegisterAcceptancePolicy(ctx, policy)
	if err != nil || !proto.Equal(published, grpcPolicy) {
		t.Fatal("gRPC policy replay", grpcPolicy, err)
	}
	replay, err := engine.RegisterAcceptancePolicy(ctx, author, model.PolicyRequest{Command: model.Command{Key: "policy", Purpose: "review"}, Policy: model.AcceptancePolicy{Scope: "case", ID: "policy", Mode: "automated", SourceClasses: []string{"customer"}, AcceptingServices: []string{acceptor.Principal}, Required: []model.AttestationRequirement{{Kind: "human_review", Principals: []string{reviewer.Principal, author.Principal}, RequireExpiry: true, MaxAgeSeconds: 3600}}}})
	if err != nil || replay.ID != published.Result.Id || replay.LedgerRevision != published.Result.LedgerRevision {
		t.Fatalf("policy retry: %+v, %v", replay, err)
	}
	zero := int64(0)
	disallowUnknown := false
	schema := &v1.RegisterPredicateVersionRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "schema", Purpose: "review"}, Definition: &v1.PredicateVersion{Scope: "case", Predicate: "count", Version: "v1", PolicyId: "policy", Kind: "integer", Classification: "confidential", Cardinality: "single", TemporalPrecision: "microsecond", Constraints: &v1.ValueConstraints{AllowUnknown: &disallowUnknown, MinInteger: &zero, MaxInteger: &zero}}}
	registered, err := definitions.RegisterPredicateVersion(ctx, schema)
	if err != nil {
		t.Fatal(err)
	}
	grpcSchema, err := grpcDefinitions.RegisterPredicateVersion(ctx, schema)
	if err != nil || !proto.Equal(registered, grpcSchema) {
		t.Fatal("gRPC predicate replay", grpcSchema, err)
	}
	schemaRetry, err := engine.RegisterPredicateVersion(ctx, author, model.PredicateRequest{Command: model.Command{Key: "schema", Purpose: "review"}, Definition: model.PredicateVersion{Scope: "case", Predicate: "count", Version: "v1", PolicyID: "policy", Kind: model.Integer, Classification: "confidential", Cardinality: "single", TemporalPrecision: "microsecond", Constraints: &model.ValueConstraints{AllowUnknown: &disallowUnknown, MinInteger: &zero, MaxInteger: &zero}}})
	if err != nil || schemaRetry.LedgerRevision != registered.Result.LedgerRevision {
		t.Fatalf("schema presence parity: %+v, %v", schemaRetry, err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	object, err := store.Put(ctx, author.Tenant, "source", []byte("0"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.RegisterEvidence(ctx, author, model.EvidenceRequest{Command: model.Command{Key: "source", Purpose: "review"}, Evidence: model.Evidence{ID: "source", Scope: "case", SourceSystem: "test", SourceObject: "source", SourceRevision: "1", Speaker: "customer", SourceClass: "customer", MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "test", SourceTime: at, Object: object}})
	if err != nil {
		t.Fatal(err)
	}
	factClient := memoryv1connect.NewFactServiceClient(authorRPC)
	proposal := &v1.ProposeFactRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "proposal", Purpose: "review"}, Key: &v1.FactKey{Scope: "case", Subject: "subject", Predicate: "count"}, SchemaVersion: "v1", Value: &v1.Value{Payload: &v1.Value_Integer{Integer: 1}}, Valid: &v1.Interval{From: timestamppb.New(at)}, Sources: []*v1.SourceSpan{{EvidenceId: "source", Start: 0, End: 1, Quote: "0"}}}
	if _, err := factClient.ProposeFact(ctx, proposal); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("zero upper bound lost: %v", err)
	}
	if result, err := grpcFacts.ProposeFact(ctx, proposal); result != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal("gRPC zero upper bound lost", result, err)
	}
	proposal.Value = &v1.Value{Payload: &v1.Value_Unknown{Unknown: &v1.UnknownValue{}}}
	if _, err := factClient.ProposeFact(ctx, proposal); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("false allow_unknown lost: %v", err)
	}
	if result, err := grpcFacts.ProposeFact(ctx, proposal); result != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal("gRPC false allow_unknown lost", result, err)
	}
	proposal.Value = &v1.Value{Payload: &v1.Value_Integer{Integer: 0}}
	proposed, err := factClient.ProposeFact(ctx, proposal)
	if err != nil {
		t.Fatal(err)
	}
	grpcProposed, err := grpcFacts.ProposeFact(ctx, proposal)
	if err != nil || !proto.Equal(proposed, grpcProposed) {
		t.Fatal("gRPC zero-valued proposal replay", grpcProposed, err)
	}
	acceptance := &v1.AcceptFactRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "accept", Purpose: "review"}, Scope: "case", RecordId: proposed.Result.RecordId, VersionId: proposed.Result.VersionId, ExpectedRevision: proposed.Result.StreamRevision}
	acceptClient := memoryv1connect.NewFactServiceClient(governanceClient(t, engine, acceptor))
	if _, err := acceptClient.AcceptFact(ctx, acceptance); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("missing review accepted: %v", err)
	}
	if result, err := grpcAccept.AcceptFact(ctx, acceptance); result != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatal("gRPC missing review accepted", result, err)
	}
	expiry := at.Add(time.Hour)
	attestation := &v1.RecordAttestationRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "review", Purpose: "review"}, Attestation: &v1.AttestationInput{Id: "review", Scope: "case", RecordId: proposed.Result.RecordId, VersionId: proposed.Result.VersionId, ContentDigest: proposed.Result.ContentDigest, Kind: "human_review", EvidenceId: "source", ExpiresAt: timestamppb.New(expiry)}}
	own := memoryv1connect.NewAttestationServiceClient(authorRPC)
	if _, err := own.RecordAttestation(ctx, attestation); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("self review accepted: %v", err)
	}
	if result, err := grpcOwn.RecordAttestation(ctx, attestation); result != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatal("gRPC self review accepted", result, err)
	}
	reviewClient := memoryv1connect.NewAttestationServiceClient(governanceClient(t, engine, reviewer))
	reviewed, err := grpcReview.RecordAttestation(ctx, attestation)
	if err != nil {
		t.Fatal(err)
	}
	connectReviewed, err := reviewClient.RecordAttestation(ctx, attestation)
	if err != nil || !proto.Equal(reviewed, connectReviewed) {
		t.Fatal("gRPC/Connect attestation replay", connectReviewed, err)
	}
	reviewRetry, err := engine.RecordAttestation(ctx, reviewer, model.AttestationRequest{Command: model.Command{Key: "review", Purpose: "review"}, Attestation: model.Attestation{ID: "review", Scope: "case", RecordID: proposed.Result.RecordId, VersionID: proposed.Result.VersionId, ContentDigest: proposed.Result.ContentDigest, Kind: "human_review", EvidenceID: "source", Attestor: reviewer, ExpiresAt: &expiry}})
	if err != nil || reviewRetry.ID != reviewed.Result.Id || reviewRetry.LedgerRevision != reviewed.Result.LedgerRevision {
		t.Fatalf("review actor/retry parity: %+v, %v", reviewRetry, err)
	}
	accepted, err := grpcAccept.AcceptFact(ctx, acceptance)
	if err != nil {
		t.Fatal(err)
	}
	connectAccepted, err := acceptClient.AcceptFact(ctx, acceptance)
	if err != nil || !proto.Equal(accepted, connectAccepted) {
		t.Fatal("gRPC/Connect acceptance replay", connectAccepted, err)
	}
	query := &v1.GetFactRequest{Tenant: author.Tenant, Purpose: "review", Key: proposal.Key, ValidAt: proposal.Valid.From}
	resolved, err := factClient.GetFact(ctx, query)
	if err != nil || resolved.Result.State != "resolved" || resolved.Result.Value.GetInteger() != 0 {
		t.Fatalf("reviewed value: %v, %v", resolved, err)
	}
	grpcResolved, err := grpcFacts.GetFact(ctx, query)
	if err != nil || !proto.Equal(resolved, grpcResolved) {
		t.Fatal("gRPC resolved zero value", grpcResolved, err)
	}
	withdrawal := &v1.WithdrawAttestationRequest{Tenant: author.Tenant, Command: &v1.Command{Key: "withdraw", Purpose: "review"}, Scope: "case", AttestationId: "review"}
	withdrawn, err := grpcReview.WithdrawAttestation(ctx, withdrawal)
	if err != nil {
		t.Fatal(err)
	}
	connectWithdrawn, err := reviewClient.WithdrawAttestation(ctx, withdrawal)
	if err != nil || !proto.Equal(withdrawn, connectWithdrawn) {
		t.Fatal("gRPC/Connect withdrawal replay", connectWithdrawn, err)
	}
	withdrawalRetry, err := engine.WithdrawAttestation(ctx, reviewer, model.Withdrawal{Command: model.Command{Key: "withdraw", Purpose: "review"}, Scope: "case", AttestationID: "review"})
	if err != nil || withdrawalRetry.LedgerRevision != withdrawn.Result.LedgerRevision {
		t.Fatalf("withdrawal retry: %+v, %v", withdrawalRetry, err)
	}
	ineligible, err := factClient.GetFact(ctx, query)
	if err != nil || ineligible.Result.State != "ineligible" || ineligible.Result.Value != nil {
		t.Fatalf("withdrawal eligibility: %v, %v", ineligible, err)
	}
	grpcIneligible, err := grpcFacts.GetFact(ctx, query)
	if err != nil || !proto.Equal(ineligible, grpcIneligible) {
		t.Fatal("gRPC withdrawn eligibility", grpcIneligible, err)
	}
}

func testRemoteDelegation(t *testing.T, ctx context.Context, engine, unconfigured *facts.Engine, recorder model.Caller, request model.DelegatedAttestationRequest, committed model.CommitResult) {
	t.Helper()
	client := memoryv1connect.NewAttestationServiceClient(governanceClient(t, engine, recorder))
	grpcClient := v1.NewAttestationServiceClient(governanceGRPC(t, engine, recorder))
	grpcMissing := v1.NewAttestationServiceClient(governanceGRPC(t, unconfigured, recorder))
	a := request.Attestation
	confirmation := a.Confirmation
	input := &v1.AttestationInput{Id: a.ID, Scope: a.Scope, RecordId: a.RecordID, VersionId: a.VersionID, ContentDigest: a.ContentDigest, Kind: a.Kind, EvidenceId: a.EvidenceID, Confirmation: &v1.ClientConfirmation{
		Presentation:        &v1.ArtifactReference{EvidenceId: confirmation.Presentation.EvidenceID, Digest: confirmation.Presentation.Digest},
		Response:            &v1.ArtifactReference{EvidenceId: confirmation.Response.EvidenceID, Digest: confirmation.Response.Digest},
		PresentationVersion: confirmation.PresentationVersion, PresentedAt: timestamppb.New(confirmation.PresentedAt), RespondedAt: timestamppb.New(confirmation.RespondedAt),
	}}
	if a.ExpiresAt != nil {
		input.ExpiresAt = timestamppb.New(*a.ExpiresAt)
	}
	rpcRequest := &v1.RecordDelegatedAttestationRequest{Tenant: recorder.Tenant, Command: &v1.Command{Key: request.Key, Purpose: request.Purpose, CorrelationId: request.CorrelationID}, Attestation: input, AttestorPrincipal: a.Attestor.Principal, GrantId: request.GrantID}
	retry, err := client.RecordDelegatedAttestation(ctx, rpcRequest)
	if err != nil || retry.Result.Id != committed.ID || retry.Result.LedgerRevision != committed.LedgerRevision || retry.Result.ContentDigest != committed.ContentDigest {
		t.Fatalf("delegated remote replay: %v, %v", retry, err)
	}
	grpcRetry, err := grpcClient.RecordDelegatedAttestation(ctx, rpcRequest)
	if err != nil || !proto.Equal(retry, grpcRetry) {
		t.Fatal("gRPC delegated attestation replay", grpcRetry, err)
	}
	rpcRequest.AttestorPrincipal = "forged-client"
	if _, err := client.RecordDelegatedAttestation(ctx, rpcRequest); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("forged identity claim: %v", err)
	}
	if result, err := grpcClient.RecordDelegatedAttestation(ctx, rpcRequest); result != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatal("gRPC forged identity claim", result, err)
	}
	rpcRequest.AttestorPrincipal = a.Attestor.Principal
	missing := memoryv1connect.NewAttestationServiceClient(governanceClient(t, unconfigured, recorder))
	if _, err := missing.RecordDelegatedAttestation(ctx, rpcRequest); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("missing grant verifier: %v", err)
	}
	if result, err := grpcMissing.RecordDelegatedAttestation(ctx, rpcRequest); result != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatal("gRPC missing grant verifier", result, err)
	}
	if _, err := client.RecordAttestation(ctx, &v1.RecordAttestationRequest{Tenant: recorder.Tenant, Command: rpcRequest.Command, Attestation: input}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("direct service confirmation bypass: %v", err)
	}
	if result, err := grpcClient.RecordAttestation(ctx, &v1.RecordAttestationRequest{Tenant: recorder.Tenant, Command: rpcRequest.Command, Attestation: input}); result != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal("gRPC direct service confirmation bypass", result, err)
	}
}
