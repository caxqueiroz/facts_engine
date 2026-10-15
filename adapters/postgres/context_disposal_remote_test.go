package postgres_test

import (
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"context"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net/http/httptest"
	"testing"
	"time"
)

func testContextDisposalRemote(t *testing.T, ctx context.Context, engine *facts.Engine, c model.Caller, r model.ContextDisposalRequest, want model.CommitResult) {
	t.Helper()
	request := &v1.DisposeContextRequest{Tenant: c.Tenant, Command: &v1.Command{Key: r.Key, Purpose: r.Purpose, CorrelationId: r.CorrelationID}, ContextId: r.ContextID, Reason: r.Reason}
	for _, b := range r.Bindings {
		request.Bindings = append(request.Bindings, &v1.ContextRetentionBinding{Scope: b.Scope, PolicyId: b.PolicyID, Jurisdiction: b.Jurisdiction})
	}
	grpc := v1.NewRetentionServiceClient(governanceGRPC(t, engine, c))
	first, err := grpc.DisposeContext(ctx, request)
	if err != nil || first.Result.Id != want.ID || first.Result.LedgerRevision != want.LedgerRevision {
		t.Fatal("gRPC context disposal replay", first, err)
	}
	handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return c, nil }, connectapi.Limits{Timeout: 10 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := memoryv1connect.NewRetentionServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	replay, err := client.DisposeContext(ctx, request)
	if err != nil || !proto.Equal(first, replay) {
		t.Fatal("Connect context disposal replay", replay, err)
	}
	bad := proto.Clone(request).(*v1.DisposeContextRequest)
	bad.Tenant = "forged"
	if out, err := grpc.DisposeContext(ctx, bad); out != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("context tenant forgery", out, err)
	}
	bad = proto.Clone(request).(*v1.DisposeContextRequest)
	bad.Bindings = nil
	if out, err := grpc.DisposeContext(ctx, bad); out != nil || grpcstatus.Code(err) != codes.InvalidArgument {
		t.Fatal("context bindings missing", out, err)
	}
	bad = proto.Clone(request).(*v1.DisposeContextRequest)
	bad.Reason = "changed"
	if out, err := grpc.DisposeContext(ctx, bad); out != nil || grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatal("context changed replay", out, err)
	}
}
