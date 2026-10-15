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
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func testAttestationDisposalRemote(t *testing.T, ctx context.Context, engine *facts.Engine, c model.Caller, r model.AttestationDisposalRequest, want model.CommitResult) {
	t.Helper()
	request := &v1.DisposeAttestationRequest{Tenant: c.Tenant, Command: &v1.Command{Key: r.Key, Purpose: r.Purpose, CorrelationId: r.CorrelationID}, Kind: r.Kind, Scope: r.Scope, RecordId: r.RecordID, VersionId: r.VersionID, AttestationId: r.AttestationID, PolicyId: r.PolicyID, Jurisdiction: r.Jurisdiction, Purpose: r.Purpose, Reason: r.Reason}
	grpc := v1.NewRetentionServiceClient(governanceGRPC(t, engine, c))
	first, err := grpc.DisposeAttestation(ctx, request)
	if err != nil || first.Result.Id != want.ID || first.Result.StreamRevision != want.StreamRevision {
		t.Fatal("gRPC disposal replay", first, err)
	}
	handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return c, nil }, connectapi.Limits{Timeout: 10 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := memoryv1connect.NewRetentionServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	replay, err := client.DisposeAttestation(ctx, request)
	if err != nil || !proto.Equal(first, replay) {
		t.Fatal("Connect disposal replay", replay, err)
	}
	bad := proto.Clone(request).(*v1.DisposeAttestationRequest)
	bad.Tenant = "forged"
	if out, err := grpc.DisposeAttestation(ctx, bad); out != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("tenant forgery", out, err)
	}
	bad = proto.Clone(request).(*v1.DisposeAttestationRequest)
	bad.Kind = "conversation"
	if out, err := grpc.DisposeAttestation(ctx, bad); out != nil || grpcstatus.Code(err) != codes.InvalidArgument {
		t.Fatal("invalid kind", out, err)
	}
	bad = proto.Clone(request).(*v1.DisposeAttestationRequest)
	bad.Reason = "changed"
	if out, err := grpc.DisposeAttestation(ctx, bad); out != nil || grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatal("changed replay", out, err)
	}
}
