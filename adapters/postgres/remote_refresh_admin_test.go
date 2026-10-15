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
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testRemoteRefreshAdministration(t *testing.T, ctx context.Context, engine, denied *facts.Engine, caller model.Caller, scan model.MemoryRefreshScan, profile string) {
	clientFor := func(engine *facts.Engine) memoryv1connect.ExtractionServiceClient {
		t.Helper()
		handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return caller, nil }, connectapi.Limits{Timeout: 10 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		return memoryv1connect.NewExtractionServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	}
	client := clientFor(engine)
	grpcClient := v1.NewExtractionServiceClient(governanceGRPC(t, engine, caller))
	grpcDenied := v1.NewExtractionServiceClient(governanceGRPC(t, denied, caller))
	request := &v1.ScanMemoryRefreshRequest{Tenant: caller.Tenant, Scope: scan.Scope, Purpose: scan.Purpose, Trigger: &v1.MemoryRefreshTrigger{EventId: scan.Trigger.EventID, LedgerRevision: scan.Trigger.LedgerRevision}, Limit: 1}
	page, err := client.ScanMemoryRefresh(ctx, request)
	direct, directErr := engine.ScanMemoryRefresh(ctx, caller, scan)
	if err != nil || directErr != nil || len(page.Versions) != 1 || len(direct.Versions) != 1 || page.Versions[0].RecordId != direct.Versions[0].RecordID || page.Versions[0].VersionId != direct.Versions[0].VersionID {
		t.Fatal("refresh scan parity", page, err, directErr)
	}
	grpcPage, err := grpcClient.ScanMemoryRefresh(ctx, request)
	if err != nil || !proto.Equal(page, grpcPage) {
		t.Fatal("gRPC refresh scan differs", grpcPage, err)
	}
	request.AfterVersionId = page.Versions[0].VersionId
	end, err := client.ScanMemoryRefresh(ctx, request)
	if err != nil || len(end.Versions) != 0 {
		t.Fatal("refresh cursor", end, err)
	}
	grpcEnd, err := grpcClient.ScanMemoryRefresh(ctx, request)
	if err != nil || !proto.Equal(end, grpcEnd) {
		t.Fatal("gRPC refresh cursor differs", grpcEnd, err)
	}
	request.AfterVersionId = ""
	request.Trigger.LedgerRevision++
	if _, err := client.ScanMemoryRefresh(ctx, request); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatal("forged trigger", err)
	}
	if result, err := grpcClient.ScanMemoryRefresh(ctx, request); result != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatal("gRPC forged trigger", result, err)
	}
	request.Trigger.LedgerRevision--
	if _, err := clientFor(denied).ScanMemoryRefresh(ctx, request); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatal("refresh scan permission", err)
	}
	if result, err := grpcDenied.ScanMemoryRefresh(ctx, request); result != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatal("gRPC refresh scan permission", result, err)
	}
	request.Tenant = "other-tenant"
	if _, err := client.ScanMemoryRefresh(ctx, request); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatal("refresh tenant binding", err)
	}
	if result, err := grpcClient.ScanMemoryRefresh(ctx, request); result != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatal("gRPC refresh tenant binding", result, err)
	}
	request.Tenant = caller.Tenant
	request.Limit = 101
	if _, err := client.ScanMemoryRefresh(ctx, request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatal("unbounded scan", err)
	}
	if result, err := grpcClient.ScanMemoryRefresh(ctx, request); result != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal("gRPC unbounded scan", result, err)
	}
	when := time.Now().UTC().Truncate(time.Hour)
	sweep := &v1.StartMemoryRefreshSweepRequest{Tenant: caller.Tenant, Command: &v1.Command{Key: "remote-refresh-sweep", Purpose: scan.Purpose}, Scope: scan.Scope, Stream: "remote-admin", ProfileId: profile, ScheduledFor: timestamppb.New(when), IntervalMinutes: 60}
	result, err := grpcClient.StartMemoryRefreshSweep(ctx, sweep)
	if err != nil {
		t.Fatal("start sweep", err)
	}
	replay, err := client.StartMemoryRefreshSweep(ctx, sweep)
	if err != nil || !proto.Equal(result, replay) {
		t.Fatal("sweep replay", replay, err)
	}
	embedded, err := engine.StartMemoryRefreshSweep(ctx, caller, model.MemoryRefreshSweepRequest{Command: model.Command{Key: sweep.Command.Key, Purpose: scan.Purpose}, Scope: scan.Scope, Stream: sweep.Stream, ProfileID: profile, ScheduledFor: when, Interval: time.Hour})
	if err != nil || embedded.ID != result.Result.Id || embedded.LedgerRevision != result.Result.LedgerRevision {
		t.Fatal("embedded sweep replay", embedded, err)
	}
	changed := proto.Clone(sweep).(*v1.StartMemoryRefreshSweepRequest)
	changed.Stream = "different-stream"
	if _, err := client.StartMemoryRefreshSweep(ctx, changed); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatal("changed sweep replay", err)
	}
	if result, err := grpcClient.StartMemoryRefreshSweep(ctx, changed); result != nil || status.Code(err) != codes.AlreadyExists {
		t.Fatal("gRPC changed sweep replay", result, err)
	}
	changed = proto.Clone(sweep).(*v1.StartMemoryRefreshSweepRequest)
	changed.ScheduledFor.Nanos = 1
	if _, err := client.StartMemoryRefreshSweep(ctx, changed); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatal("timestamp precision", err)
	}
	if result, err := grpcClient.StartMemoryRefreshSweep(ctx, changed); result != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal("gRPC timestamp precision", result, err)
	}
	changed = proto.Clone(sweep).(*v1.StartMemoryRefreshSweepRequest)
	changed.ScheduledFor = timestamppb.New(when.Add(24 * time.Hour))
	if _, err := client.StartMemoryRefreshSweep(ctx, changed); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatal("future sweep", err)
	}
	if result, err := grpcClient.StartMemoryRefreshSweep(ctx, changed); result != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatal("gRPC future sweep", result, err)
	}
}
