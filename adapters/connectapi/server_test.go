package connectapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testEngine struct {
	Engine
	calls atomic.Int64
	get   func(context.Context, model.Caller, model.FactQuery) (model.FactResult, error)
}

func (e *testEngine) GetFact(ctx context.Context, c model.Caller, q model.FactQuery) (model.FactResult, error) {
	e.calls.Add(1)
	return e.get(ctx, c, q)
}
func testLimits() Limits {
	return Limits{Timeout: time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20}
}
func validQuery() *v1.GetFactRequest {
	return &v1.GetFactRequest{Tenant: "tenant", Purpose: "review", Key: &v1.FactKey{Scope: "scope", Subject: "subject", Predicate: "predicate"}, ValidAt: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))}
}
func startServer(t *testing.T, e Engine, verify IdentityVerifier, limits Limits) (*httptest.Server, memoryv1connect.FactServiceClient) {
	t.Helper()
	handler, err := NewHandler(e, verify, limits)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	client := memoryv1connect.NewFactServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	return server, client
}
func fixedIdentity(context.Context) (model.Caller, error) {
	return model.Caller{Tenant: "tenant", Principal: "human"}, nil
}

func TestConnectAndGRPC(t *testing.T) {
	engine := &testEngine{get: func(_ context.Context, c model.Caller, q model.FactQuery) (model.FactResult, error) {
		if c.Tenant != "tenant" || c.Principal != "human" || q.Key.Subject != "subject" {
			return model.FactResult{}, errors.New("incorrect mapping")
		}
		if q.Purpose == "conflict" {
			return model.FactResult{}, &model.RevisionConflict{RecordID: "record", ExpectedRevision: 1, CurrentRevision: 2}
		}
		if q.Purpose == "denied" {
			return model.FactResult{}, model.ErrDenied
		}
		b := false
		return model.FactResult{State: "accepted", Value: &model.Value{Kind: model.Boolean, Boolean: &b}, LedgerRevision: 42}, nil
	}}
	server, client := startServer(t, engine, fixedIdentity, testLimits())
	got, err := client.GetFact(context.Background(), validQuery())
	if err != nil || got.Result.LedgerRevision != 42 {
		t.Fatalf("Connect: %v, %v", got, err)
	}
	if _, ok := got.Result.Value.Payload.(*v1.Value_Boolean); !ok {
		t.Fatal("false lost presence")
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	conn, err := grpc.NewClient(strings.TrimPrefix(server.URL, "https://"), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	grpcClient := v1.NewFactServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g, err := grpcClient.GetFact(ctx, validQuery())
	if err != nil || g.Result.LedgerRevision != 42 {
		t.Fatalf("gRPC: %v, %v", g, err)
	}
	denied := validQuery()
	denied.Purpose = "denied"
	if _, err := grpcClient.GetFact(ctx, denied); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("gRPC error: %v", err)
	}
	conflict := validQuery()
	conflict.Purpose = "conflict"
	_, err = grpcClient.GetFact(ctx, conflict)
	if status.Code(err) != codes.Aborted || len(status.Convert(err).Details()) != 1 {
		t.Fatalf("gRPC conflict: %v; details=%v", err, status.Convert(err).Details())
	}
	_, err = client.GetFact(ctx, conflict)
	var rpc *connect.Error
	if !errors.As(err, &rpc) || len(rpc.Details()) != 1 {
		t.Fatalf("Connect conflict: %v; details=%v", err, rpc.Details())
	}

}

func TestIdentityAndValidationBeforeEngine(t *testing.T) {
	engine := &testEngine{get: func(context.Context, model.Caller, model.FactQuery) (model.FactResult, error) {
		return model.FactResult{}, nil
	}}
	_, client := startServer(t, engine, fixedIdentity, testLimits())
	for _, tc := range []struct {
		name   string
		change func(*v1.GetFactRequest)
		code   connect.Code
	}{
		{"tenant", func(r *v1.GetFactRequest) { r.Tenant = "other" }, connect.CodePermissionDenied},
		{"key", func(r *v1.GetFactRequest) { r.Key = nil }, connect.CodeInvalidArgument},
		{"purpose", func(r *v1.GetFactRequest) { r.Purpose = "" }, connect.CodeInvalidArgument},
		{"timestamp", func(r *v1.GetFactRequest) { r.ValidAt = &timestamppb.Timestamp{Nanos: 1000000000} }, connect.CodeInvalidArgument},
		{"boundary", func(r *v1.GetFactRequest) { r.KnownAt = r.ValidAt; r.KnowledgeRevision = 1 }, connect.CodeInvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := validQuery()
			tc.change(r)
			_, err := client.GetFact(context.Background(), r)
			if connect.CodeOf(err) != tc.code {
				t.Fatalf("got %v", err)
			}
		})
	}
	if engine.calls.Load() != 0 {
		t.Fatal("invalid requests reached engine")
	}
	_, unauth := startServer(t, engine, func(context.Context) (model.Caller, error) { return model.Caller{}, errors.New("private credential") }, testLimits())
	_, err := unauth.GetFact(context.Background(), &v1.GetFactRequest{})
	if connect.CodeOf(err) != connect.CodeUnauthenticated || strings.Contains(err.Error(), "private") {
		t.Fatalf("authentication: %v", err)
	}
}

func TestDeadlineAndErrorSanitization(t *testing.T) {
	engine := &testEngine{get: func(ctx context.Context, _ model.Caller, _ model.FactQuery) (model.FactResult, error) {
		<-ctx.Done()
		return model.FactResult{}, ctx.Err()
	}}
	limits := testLimits()
	limits.Timeout = 30 * time.Millisecond
	_, client := startServer(t, engine, fixedIdentity, limits)
	_, err := client.GetFact(context.Background(), validQuery())
	if connect.CodeOf(err) != connect.CodeDeadlineExceeded {
		t.Fatalf("deadline: %v", err)
	}
	if err := rpcError(errors.New("secret sql password")); connect.CodeOf(err) != connect.CodeInternal || strings.Contains(err.Error(), "secret") {
		t.Fatalf("internal: %v", err)
	}
}

func TestAdmissionAndRequestSize(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	engine := &testEngine{get: func(ctx context.Context, _ model.Caller, _ model.FactQuery) (model.FactResult, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return model.FactResult{}, nil
		case <-ctx.Done():
			return model.FactResult{}, ctx.Err()
		}
	}}
	limits := testLimits()
	limits.MaxConcurrent = 1
	limits.Timeout = 5 * time.Second
	_, client := startServer(t, engine, fixedIdentity, limits)
	done := make(chan error, 1)
	go func() { _, err := client.GetFact(context.Background(), validQuery()); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not enter")
	}
	_, err := client.GetFact(context.Background(), validQuery())
	close(release)
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("admission: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	limits.MaxRequestBytes = 32
	_, small := startServer(t, engine, fixedIdentity, limits)
	_, err = small.GetFact(context.Background(), validQuery())
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("request bytes: %v", err)
	}
	if engine.calls.Load() != 1 {
		t.Fatal("rejected request entered engine")
	}
}

func TestReadRequestValidation(t *testing.T) {
	engine := &testEngine{}
	server, client := startServer(t, engine, fixedIdentity, testLimits())
	snapshots := memoryv1connect.NewSnapshotServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	query := validQuery()
	list := &v1.ListFactsRequest{Tenant: query.Tenant, Scope: query.Key.Scope, Purpose: query.Purpose, ValidAt: query.ValidAt}
	for _, limit := range []int32{0, -1, 101} {
		list.Limit = limit
		if _, err := client.ListFacts(context.Background(), list); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("list limit %d: %v", limit, err)
		}
	}
	list.Limit = 1
	list.KnowledgeRevision = 2
	list.KnownAt = query.ValidAt
	if _, err := client.ListFacts(context.Background(), list); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("list boundary: %v", err)
	}
	history := &v1.GetFactHistoryRequest{Tenant: query.Tenant, Scope: query.Key.Scope, RecordId: "record", Purpose: query.Purpose, Limit: 101}
	if _, err := client.GetFactHistory(context.Background(), history); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("history limit: %v", err)
	}
	history.Limit = 1
	history.PageToken = strings.Repeat("x", 4097)
	if _, err := client.GetFactHistory(context.Background(), history); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("history token size: %v", err)
	}
	snapshot := &v1.CreateFactSnapshotRequest{Tenant: query.Tenant, Command: &v1.Command{Key: "snapshot", Purpose: query.Purpose}, ValidAt: query.ValidAt}
	if _, err := snapshots.CreateFactSnapshot(context.Background(), snapshot); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("empty selectors: %v", err)
	}
	for range 129 {
		snapshot.Selectors = append(snapshot.Selectors, query.Key)
	}
	if _, err := snapshots.CreateFactSnapshot(context.Background(), snapshot); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("selector count: %v", err)
	}
	if _, err := snapshots.GetFactSnapshot(context.Background(), &v1.GetFactSnapshotRequest{Tenant: "other", Id: "snapshot", Purpose: query.Purpose}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("snapshot tenant: %v", err)
	}
}

func TestResponseTimestampRange(t *testing.T) {
	converter := responseConverter{}
	converter.snapshot(model.FactSnapshot{EvaluatedAt: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)})
	if converter.err == nil || connect.CodeOf(rpcError(converter.err)) != connect.CodeInternal {
		t.Fatalf("invalid output timestamp: %v", converter.err)
	}
}

func TestEvidenceRequestValidation(t *testing.T) {
	server, _ := startServer(t, &testEngine{}, fixedIdentity, testLimits())
	client := memoryv1connect.NewEvidenceServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	object := &v1.ObjectRef{Locator: "object", Version: "v1", Digest: strings.Repeat("a", 64), Size: 1}
	request := &v1.RegisterRedactedEvidenceRequest{Tenant: "tenant", Command: &v1.Command{Key: "redact", Purpose: "review"}, Id: "derived", Scope: "scope", ParentEvidenceId: "parent", PolicyVersion: "policy", Object: object}
	request.Ranges = []*v1.EvidenceCopyRange{{OriginalStart: 2, OriginalEnd: 1}}
	if _, err := client.RegisterRedactedEvidence(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("reversed range: %v", err)
	}
	request.Ranges = nil
	for range 257 {
		request.Ranges = append(request.Ranges, &v1.EvidenceCopyRange{OriginalEnd: 1})
	}
	if _, err := client.RegisterRedactedEvidence(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("range count: %v", err)
	}
	request.Ranges = nil
	request.Object.Size = 16777217
	if _, err := client.RegisterRedactedEvidence(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("object size: %v", err)
	}
	request.Object.Size = 1
	request.Object.Digest = "not-a-digest"
	if _, err := client.RegisterRedactedEvidence(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("digest: %v", err)
	}
	if _, err := client.GetEvidenceContent(context.Background(), &v1.GetEvidenceContentRequest{Tenant: "other", Scope: "scope", Id: "id", Purpose: "review"}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("evidence tenant: %v", err)
	}
}

func TestGovernanceRequestValidation(t *testing.T) {
	server, _ := startServer(t, &testEngine{}, fixedIdentity, testLimits())
	transport := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL))
	definitions := memoryv1connect.NewDefinitionServiceClient(transport)
	attestations := memoryv1connect.NewAttestationServiceClient(transport)
	command := &v1.Command{Key: "command", Purpose: "review"}
	policy := &v1.RegisterAcceptancePolicyRequest{Tenant: "tenant", Command: command, Policy: &v1.AcceptancePolicy{Id: "policy", Scope: "scope", SourceClasses: []string{"customer", "customer"}}}
	if _, err := definitions.RegisterAcceptancePolicy(context.Background(), policy); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("duplicate source classes: %v", err)
	}
	negative := int32(-1)
	schema := &v1.RegisterPredicateVersionRequest{Tenant: "tenant", Command: command, Definition: &v1.PredicateVersion{Scope: "scope", Predicate: "name", Version: "v1", PolicyId: "policy", Kind: "text", Classification: "confidential", Constraints: &v1.ValueConstraints{MaxTextRunes: &negative}}}
	if _, err := definitions.RegisterPredicateVersion(context.Background(), schema); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("negative constraint: %v", err)
	}
	attestation := &v1.AttestationInput{Id: "attestation", Scope: "scope", RecordId: "record", VersionId: "version", Kind: "client_confirmation", ContentDigest: strings.Repeat("a", 64), EvidenceId: "response", Confirmation: &v1.ClientConfirmation{}}
	if _, err := attestations.RecordAttestation(context.Background(), &v1.RecordAttestationRequest{Tenant: "tenant", Command: command, Attestation: attestation}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("missing confirmation artifacts: %v", err)
	}
	attestation.Kind = "human_review"
	attestation.Confirmation = nil
	if _, err := attestations.RecordDelegatedAttestation(context.Background(), &v1.RecordDelegatedAttestationRequest{Tenant: "tenant", Command: command, Attestation: attestation, AttestorPrincipal: "client"}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("missing grant: %v", err)
	}
	if _, err := attestations.WithdrawAttestation(context.Background(), &v1.WithdrawAttestationRequest{Tenant: "other", Command: command, Scope: "scope", AttestationId: "attestation"}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("withdrawal tenant: %v", err)
	}
}

func TestMemoryRequestValidation(t *testing.T) {
	server, _ := startServer(t, &testEngine{}, fixedIdentity, testLimits())
	client := memoryv1connect.NewMemoryServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	request := &v1.ProposeMemoryRequest{Tenant: "tenant", Proposal: &v1.MemoryProposal{Command: &v1.Command{Key: "proposal", Purpose: "review"}, Key: &v1.MemoryKey{Scope: "scope", Id: "memory"}, Subtype: "summary", SchemaVersion: "v1", Valid: &v1.Interval{}, Sources: []*v1.SourceSpan{{EvidenceId: "source", End: 1, Quote: "x"}}}}
	if _, err := client.ProposeMemory(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("empty body: %v", err)
	}
	request.Proposal.Body = "summary"
	request.Proposal.Dependencies = []*v1.MemoryDependency{{Scope: "scope", RecordId: "record", VersionId: "version", ContentDigest: strings.Repeat("a", 64), Label: "invented"}}
	if _, err := client.ProposeMemory(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("dependency label: %v", err)
	}
	request.Proposal.Dependencies = nil
	request.Proposal.Command = nil
	if _, err := client.ProposeMemory(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("missing nested command: %v", err)
	}
	if _, err := client.GetMemoryHistory(context.Background(), &v1.GetMemoryHistoryRequest{Tenant: "tenant", Scope: "scope", RecordId: "record", Purpose: "review", Limit: 101}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("history size: %v", err)
	}
	if _, err := client.GetMemoryVersion(context.Background(), &v1.GetMemoryVersionRequest{Tenant: "other", Scope: "scope", RecordId: "record", VersionId: "version", Purpose: "review"}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("version tenant: %v", err)
	}
}

func TestIngestionAndExtractionValidation(t *testing.T) {
	server, _ := startServer(t, &testEngine{}, fixedIdentity, testLimits())
	transport := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL))
	ingestion := memoryv1connect.NewIngestionServiceClient(transport)
	extraction := memoryv1connect.NewExtractionServiceClient(transport)
	command := &v1.Command{Key: "command", Purpose: "review"}
	conversation := &v1.IngestConversationRequest{Tenant: "tenant", Conversation: &v1.ConversationInput{Command: command, Scope: "scope", SourceSystem: "chat", ConversationId: "chat", Revision: "v1", Timezone: "UTC"}}
	if _, err := ingestion.IngestConversation(context.Background(), conversation); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("empty conversation: %v", err)
	}
	conversation.Conversation.Messages = []*v1.ConversationMessage{{Id: "message", Revision: "v1", Speaker: "customer", SpeakerRole: "customer", SourceClass: "customer", SourceTime: validQuery().ValidAt, MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "retain", Object: &v1.ObjectRef{Locator: "object", Version: "v1", Digest: strings.Repeat("a", 64), Size: (1 << 20) + 1}}}
	if _, err := ingestion.IngestConversation(context.Background(), conversation); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("message size: %v", err)
	}
	request := &v1.StartExtractionRequest{Tenant: "tenant", Command: command, Scope: "scope", ConversationId: "chat", ProfileId: "profile"}
	if _, err := extraction.StartExtraction(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("missing targets: %v", err)
	}
	request.Subjects = []string{"subject"}
	request.Predicates = []*v1.PredicateBinding{{Predicate: "predicate", SchemaVersion: "v1"}}
	request.MemoryTargets = []*v1.MemoryExtractionTarget{{Id: "memory", Subtype: "summary", SchemaVersion: "v1"}}
	if _, err := extraction.StartExtraction(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("mixed extraction modes: %v", err)
	}
	request.MemoryTargets = nil
	request.InputRanges = []*v1.ExtractionInputRange{{EvidenceId: "source", Start: 2, End: 1}}
	if _, err := extraction.StartExtraction(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("reversed range: %v", err)
	}
	if _, err := extraction.PlanExtractionChunks(context.Background(), &v1.PlanExtractionChunksRequest{Tenant: "tenant", Scope: "scope", ConversationId: "chat", ProfileId: "profile", Purpose: "review", MaxSourceBytes: 3}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("chunk size: %v", err)
	}
	if _, err := extraction.RetryJob(context.Background(), &v1.RetryJobRequest{Tenant: "tenant", Command: command, Scope: "scope", Id: "job", ExpectedFence: -1}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("negative fence: %v", err)
	}
	if _, err := extraction.GetExtractionRun(context.Background(), &v1.GetExtractionRunRequest{Tenant: "other", Scope: "scope", Id: "attempt", Purpose: "review"}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("run tenant: %v", err)
	}
}

func TestCalculationAndAuditValidation(t *testing.T) {
	server, _ := startServer(t, &testEngine{}, fixedIdentity, testLimits())
	transport := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL))
	calculations := memoryv1connect.NewCalculationServiceClient(transport)
	audit := memoryv1connect.NewAuditServiceClient(transport)
	digest := strings.Repeat("a", 64)
	receipt := &v1.CalculationReceipt{
		Command: &v1.Command{Key: "command", Purpose: "review"}, Scope: "scope", SnapshotId: "snapshot",
		ApplicationId: "app", ExternalRunId: "run", MethodId: "method", MethodVersion: "v1",
		ExecutableDigest: digest, ConfigurationDigest: digest, NormalizationVersion: "v1",
		Input: &v1.ArtifactReference{EvidenceId: "input", Digest: digest}, Output: &v1.ArtifactReference{EvidenceId: "output", Digest: digest},
		StartedAt: timestamppb.New(time.Unix(100, 0)), CompletedAt: timestamppb.New(time.Unix(99, 0)), Outcome: "succeeded",
	}
	request := &v1.RecordCalculationRequest{Tenant: "tenant", Receipt: receipt}
	if _, err := calculations.RecordCalculation(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("reversed calculation times: %v", err)
	}
	receipt.CompletedAt = receipt.StartedAt
	receipt.Input = nil
	if _, err := calculations.RecordCalculation(context.Background(), request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("missing artifact: %v", err)
	}
	for _, limit := range []int32{0, 1001} {
		if _, err := audit.ListChanges(context.Background(), &v1.ListChangesRequest{Tenant: "tenant", Scope: "scope", Purpose: "review", Limit: limit}); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("change limit %d: %v", limit, err)
		}
	}
	if _, err := audit.GetAuditExport(context.Background(), &v1.GetAuditExportRequest{Tenant: "other", Scope: "scope", Id: "export", Purpose: "review"}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("audit tenant: %v", err)
	}
}

func TestRetrievalValidation(t *testing.T) {
	server, _ := startServer(t, &testEngine{}, fixedIdentity, testLimits())
	client := memoryv1connect.NewRetrievalServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	query := &v1.RecallQuery{Scopes: []string{"scope", "scope"}, Query: "query", Purpose: "review", ValidAt: validQuery().ValidAt, Limit: 20}
	if _, err := client.Recall(context.Background(), &v1.RecallRequest{Tenant: "tenant", Query: query}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("duplicate scopes: %v", err)
	}
	query.Scopes = []string{"scope"}
	query.Limit = 21
	if _, err := client.RecallMemories(context.Background(), &v1.RecallMemoriesRequest{Tenant: "tenant", Query: query}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("recall limit: %v", err)
	}
	query.Limit = 20
	query.Query = strings.Repeat("x", 4097)
	if _, err := client.Recall(context.Background(), &v1.RecallRequest{Tenant: "tenant", Query: query}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("query size: %v", err)
	}
	if _, err := client.WaitForProjection(context.Background(), &v1.WaitForProjectionRequest{Tenant: "tenant", Scopes: []string{"scope"}, Generation: "g1", Purpose: "review"}); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("zero revision: %v", err)
	}
	if _, err := client.Recall(context.Background(), &v1.RecallRequest{Tenant: "other", Query: query}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("recall tenant: %v", err)
	}
}
