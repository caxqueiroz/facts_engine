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

func testDisposalBatchRemote(t *testing.T, ctx context.Context, engine *facts.Engine, c model.Caller, r model.DisposalBatchRequest, want model.CommitResult) {
	t.Helper()
	request := &v1.DisposeBatchRequest{Tenant: c.Tenant, Command: &v1.Command{Key: r.Key, Purpose: r.Purpose, CorrelationId: r.CorrelationID}, Scope: r.Scope, Reason: r.Reason}
	for _, v := range r.Versions {
		selection := &v1.BatchVersionSelection{Kind: v.Kind, Scope: v.Scope, RecordId: v.RecordID, VersionId: v.VersionID}
		for _, b := range v.Bindings {
			selection.Bindings = append(selection.Bindings, &v1.VersionRetentionBinding{Scope: b.Scope, PolicyId: b.PolicyID, Jurisdiction: b.Jurisdiction})
		}
		request.Versions = append(request.Versions, selection)
	}
	for _, e := range r.Extractions {
		selection := &v1.BatchExtractionSelection{JobId: e.JobID}
		for _, b := range e.Bindings {
			selection.Bindings = append(selection.Bindings, &v1.ExtractionRetentionBinding{Scope: b.Scope, JobPolicyId: b.JobPolicyID, RequestPolicyId: b.RequestPolicyID, ResponsePolicyId: b.ResponsePolicyID, Jurisdiction: b.Jurisdiction})
		}
		request.Extractions = append(request.Extractions, selection)
	}
	for _, item := range r.Snapshots {
		selection := &v1.BatchSnapshotSelection{SnapshotId: item.SnapshotID}
		for _, b := range item.Bindings {
			selection.Bindings = append(selection.Bindings, &v1.SnapshotRetentionBinding{Scope: b.Scope, PolicyId: b.PolicyID, Jurisdiction: b.Jurisdiction})
		}
		request.Snapshots = append(request.Snapshots, selection)
	}
	for _, item := range r.Contexts {
		selection := &v1.BatchContextSelection{ContextId: item.ContextID}
		for _, b := range item.Bindings {
			selection.Bindings = append(selection.Bindings, &v1.ContextRetentionBinding{Scope: b.Scope, PolicyId: b.PolicyID, Jurisdiction: b.Jurisdiction})
		}
		request.Contexts = append(request.Contexts, selection)
	}
	for _, item := range r.Calculations {
		selection := &v1.BatchCalculationSelection{CalculationId: item.CalculationID}
		for _, b := range item.Bindings {
			selection.Bindings = append(selection.Bindings, &v1.CalculationRetentionBinding{Scope: b.Scope, PolicyId: b.PolicyID, Jurisdiction: b.Jurisdiction})
		}
		request.Calculations = append(request.Calculations, selection)
	}
	grpc := v1.NewRetentionServiceClient(governanceGRPC(t, engine, c))
	first, err := grpc.DisposeBatch(ctx, request)
	if err != nil || !proto.Equal(first.Result, &v1.CommitResult{Id: want.ID, RecordId: want.RecordID, VersionId: want.VersionID, RecordKind: want.RecordKind, ContentDigest: want.ContentDigest, StreamRevision: want.StreamRevision, LedgerRevision: want.LedgerRevision}) {
		t.Fatal("gRPC batch disposal replay", first, err)
	}
	handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return c, nil }, connectapi.Limits{Timeout: 10 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := memoryv1connect.NewRetentionServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	replay, err := client.DisposeBatch(ctx, request)
	if err != nil || !proto.Equal(first, replay) {
		t.Fatal("Connect batch disposal replay", replay, err)
	}
	bad := proto.Clone(request).(*v1.DisposeBatchRequest)
	bad.Tenant = "forged"
	if out, err := grpc.DisposeBatch(ctx, bad); out != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("batch tenant forgery", out, err)
	}
	bad = proto.Clone(request).(*v1.DisposeBatchRequest)
	bad.Versions = nil
	bad.Extractions = nil
	bad.Snapshots = nil
	bad.Contexts = nil
	bad.Calculations = nil
	if out, err := grpc.DisposeBatch(ctx, bad); out != nil || grpcstatus.Code(err) != codes.InvalidArgument {
		t.Fatal("batch selection missing", out, err)
	}
	bad = proto.Clone(request).(*v1.DisposeBatchRequest)
	bad.Reason = "changed"
	if out, err := grpc.DisposeBatch(ctx, bad); out != nil || grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatal("batch changed replay", out, err)
	}
}
