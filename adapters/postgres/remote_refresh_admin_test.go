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
	request := &v1.ScanMemoryRefreshRequest{Tenant: caller.Tenant, Scope: scan.Scope, Purpose: scan.Purpose, Trigger: &v1.MemoryRefreshTrigger{EventId: scan.Trigger.EventID, LedgerRevision: scan.Trigger.LedgerRevision}, Limit: 1}
	page, err := client.ScanMemoryRefresh(ctx, request)
	direct, directErr := engine.ScanMemoryRefresh(ctx, caller, scan)
	if err != nil || directErr != nil || len(page.Versions) != 1 || len(direct.Versions) != 1 || page.Versions[0].RecordId != direct.Versions[0].RecordID || page.Versions[0].VersionId != direct.Versions[0].VersionID {
		t.Fatal("refresh scan parity", page, err, directErr)
	}
	request.AfterVersionId = page.Versions[0].VersionId
	end, err := client.ScanMemoryRefresh(ctx, request)
	if err != nil || len(end.Versions) != 0 {
		t.Fatal("refresh cursor", end, err)
	}
	request.AfterVersionId = ""
	request.Trigger.LedgerRevision++
	if _, err := client.ScanMemoryRefresh(ctx, request); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatal("forged trigger", err)
	}
	request.Trigger.LedgerRevision--
	if _, err := clientFor(denied).ScanMemoryRefresh(ctx, request); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatal("refresh scan permission", err)
	}
	request.Tenant = "other-tenant"
	if _, err := client.ScanMemoryRefresh(ctx, request); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatal("refresh tenant binding", err)
	}
	request.Tenant = caller.Tenant
	request.Limit = 101
	if _, err := client.ScanMemoryRefresh(ctx, request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatal("unbounded scan", err)
	}
	when := time.Now().UTC().Truncate(time.Hour)
	sweep := &v1.StartMemoryRefreshSweepRequest{Tenant: caller.Tenant, Command: &v1.Command{Key: "remote-refresh-sweep", Purpose: scan.Purpose}, Scope: scan.Scope, Stream: "remote-admin", ProfileId: profile, ScheduledFor: timestamppb.New(when), IntervalMinutes: 60}
	result, err := client.StartMemoryRefreshSweep(ctx, sweep)
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
	changed = proto.Clone(sweep).(*v1.StartMemoryRefreshSweepRequest)
	changed.ScheduledFor.Nanos = 1
	if _, err := client.StartMemoryRefreshSweep(ctx, changed); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatal("timestamp precision", err)
	}
	changed = proto.Clone(sweep).(*v1.StartMemoryRefreshSweepRequest)
	changed.ScheduledFor = timestamppb.New(when.Add(24 * time.Hour))
	if _, err := client.StartMemoryRefreshSweep(ctx, changed); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatal("future sweep", err)
	}
}
