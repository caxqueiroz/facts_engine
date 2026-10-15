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

func testVersionDisposalRemote(t *testing.T, ctx context.Context, engine *facts.Engine, c model.Caller, r model.VersionDisposalRequest, want model.CommitResult) {
	t.Helper()
	request := &v1.DisposeVersionRequest{Tenant: c.Tenant, Command: &v1.Command{Key: r.Key, Purpose: r.Purpose, CorrelationId: r.CorrelationID}, VersionId: r.VersionID, Kind: r.Kind, Scope: r.Scope, RecordId: r.RecordID, Reason: r.Reason}
	for _, b := range r.Bindings {
		request.Bindings = append(request.Bindings, &v1.VersionRetentionBinding{Scope: b.Scope, PolicyId: b.PolicyID, Jurisdiction: b.Jurisdiction})
	}
	grpc := v1.NewRetentionServiceClient(governanceGRPC(t, engine, c))
	first, err := grpc.DisposeVersion(ctx, request)
	if err != nil || first.Result.Id != want.ID || first.Result.LedgerRevision != want.LedgerRevision {
		t.Fatal("gRPC version disposal replay", first, err)
	}
	handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return c, nil }, connectapi.Limits{Timeout: 10 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := memoryv1connect.NewRetentionServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	replay, err := client.DisposeVersion(ctx, request)
	if err != nil || !proto.Equal(first, replay) {
		t.Fatal("Connect version disposal replay", replay, err)
	}
	bad := proto.Clone(request).(*v1.DisposeVersionRequest)
	bad.Tenant = "forged"
	if out, err := grpc.DisposeVersion(ctx, bad); out != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("version tenant forgery", out, err)
	}
	bad = proto.Clone(request).(*v1.DisposeVersionRequest)
	bad.Bindings = nil
	if out, err := grpc.DisposeVersion(ctx, bad); out != nil || grpcstatus.Code(err) != codes.InvalidArgument {
		t.Fatal("version bindings missing", out, err)
	}
	bad = proto.Clone(request).(*v1.DisposeVersionRequest)
	bad.Reason = "changed"
	if out, err := grpc.DisposeVersion(ctx, bad); out != nil || grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatal("version changed replay", out, err)
	}
}
