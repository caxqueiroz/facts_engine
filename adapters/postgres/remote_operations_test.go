package postgres_test

import (
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"context"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
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

type operationsAuthorizer struct{}

func (operationsAuthorizer) Check(ctx context.Context, r model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	if r.Scope != "projection-admin" || r.Permission != "can_operate" || r.Caller.Principal != "operator" {
		return model.AuthorizationDecision{}, nil
	}
	return (allow{}).Check(ctx, r)
}

type remoteRebuild struct {
	client     memoryv1connect.OperationsServiceClient
	grpcClient v1.OperationsServiceClient
	t          *testing.T
	registry   *postgres.Ledger
}

func newRemoteRebuild(t *testing.T, ctx context.Context, l, registry *postgres.Ledger, store *evidence.Postgres, index model.RebuildIndex) *remoteRebuild {
	t.Helper()
	options := model.EngineOptions{Operations: model.OperationsOptions{Scope: "projection-admin", Store: registry, Index: index, Progress: registry}}
	e, err := facts.NewWithOptions(l, operationsAuthorizer{}, store, options)
	if err != nil {
		t.Fatal(err)
	}
	clientFor := func(c model.Caller) memoryv1connect.OperationsServiceClient {
		h, err := connectapi.NewHandler(e, func(context.Context) (model.Caller, error) { return c, nil }, connectapi.Limits{Timeout: 30 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(h)
		t.Cleanup(server.Close)
		return memoryv1connect.NewOperationsServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	}
	denied := clientFor(model.Caller{Tenant: "bank1", Principal: "reader"})
	if _, err := denied.RebuildIndex(ctx, &v1.RebuildIndexRequest{Tenant: "bank1", Generation: "g1", Purpose: "review", Action: "start", MaxAttempts: 3}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("rebuild operator: %v", err)
	}
	if _, err := denied.GetProjectionStatus(ctx, &v1.GetProjectionStatusRequest{Tenant: "bank1", Generation: "g1", Purpose: "review"}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("status operator: %v", err)
	}
	grpcDenied := v1.NewOperationsServiceClient(governanceGRPC(t, e, model.Caller{Tenant: "bank1", Principal: "reader"}))
	if result, err := grpcDenied.RebuildIndex(ctx, &v1.RebuildIndexRequest{Tenant: "bank1", Generation: "g1", Purpose: "review", Action: "start", MaxAttempts: 3}); result != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("gRPC rebuild permission", result, err)
	}
	if result, err := grpcDenied.GetProjectionStatus(ctx, &v1.GetProjectionStatusRequest{Tenant: "bank1", Generation: "g1", Purpose: "review"}); result != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("gRPC projection permission", result, err)
	}
	return &remoteRebuild{grpcClient: v1.NewOperationsServiceClient(governanceGRPC(t, e, model.Caller{Tenant: "bank1", Principal: "operator"})), client: clientFor(model.Caller{Tenant: "bank1", Principal: "operator"}), t: t, registry: registry}
}
func (r *remoteRebuild) call(ctx context.Context, k model.GenerationKey, action string, attempts int, fence int64) (model.RebuildStatus, error) {
	r.t.Helper()
	response, err := r.grpcClient.RebuildIndex(ctx, &v1.RebuildIndexRequest{Tenant: k.Tenant, Generation: k.Generation, Purpose: "verified_rebuild", Action: action, MaxAttempts: int32(attempts), ExpectedFence: fence})
	if err != nil {
		// Existing rebuild assertions use domain errors; keep those assertions while
		// executing the actual transition through the generated grpc-go client over TLS/HTTP2.
		for _, pair := range []struct {
			code codes.Code
			err  error
		}{{codes.AlreadyExists, model.ErrConflict}, {codes.Aborted, model.ErrRevision}, {codes.FailedPrecondition, model.ErrPrecondition}, {codes.Unavailable, model.ErrUnavailable}, {codes.PermissionDenied, model.ErrDenied}} {
			if grpcstatus.Code(err) == pair.code {
				return model.RebuildStatus{}, pair.err
			}
		}
		return model.RebuildStatus{}, err
	}
	s := response.Status
	direct, err := r.registry.RebuildStatus(ctx, k)
	if err != nil {
		r.t.Fatal(err)
	}
	out := model.RebuildStatus{Key: model.GenerationKey{Tenant: s.Tenant, Generation: s.Generation}, State: s.State, Fence: s.Fence, ThroughRevision: s.ThroughRevision, BaseEpoch: s.BaseEpoch, PreviousGeneration: s.PreviousGeneration, Count: int(s.Count), Digest: s.Digest, VerificationVersion: s.VerificationVersion}
	if out != direct {
		r.t.Fatalf("remote rebuild status differs: %+v, %+v", out, direct)
	}
	status, err := r.client.GetProjectionStatus(ctx, &v1.GetProjectionStatusRequest{Tenant: k.Tenant, Generation: k.Generation, Purpose: "review"})
	if err != nil {
		r.t.Fatal(err)
	}
	grpcStatus, err := r.grpcClient.GetProjectionStatus(ctx, &v1.GetProjectionStatusRequest{Tenant: k.Tenant, Generation: k.Generation, Purpose: "review"})
	if err != nil || !proto.Equal(status, grpcStatus) {
		r.t.Fatal("gRPC/Connect projection status differs", grpcStatus, err)
	}
	progress, err := r.registry.ConsumerStatus(ctx, model.ConsumerKey{Tenant: k.Tenant, Consumer: model.ProjectionConsumer(k.Generation)})
	if err != nil {
		r.t.Fatal(err)
	}
	if status.Consumer.AcknowledgedRevision != progress.AcknowledgedRevision || status.Consumer.Fence != progress.Fence || status.Rebuild.Fence != s.Fence || status.Rebuild.State != s.State {
		r.t.Fatal("projection progress mismatch")
	}
	if status.ActiveGeneration != k.Generation && !status.Incomplete {
		r.t.Fatal("candidate reported complete active coverage")
	}
	return out, nil
}
func (r *remoteRebuild) Start(ctx context.Context, k model.GenerationKey, n int) (model.RebuildStatus, error) {
	return r.call(ctx, k, "start", n, 0)
}
func (r *remoteRebuild) Verify(ctx context.Context, k model.GenerationKey) (model.RebuildStatus, error) {
	return r.call(ctx, k, "verify", 0, 0)
}
func (r *remoteRebuild) Resume(ctx context.Context, k model.GenerationKey, f int64) (model.RebuildStatus, error) {
	return r.call(ctx, k, "resume", 0, f)
}
func (r *remoteRebuild) Activate(ctx context.Context, k model.GenerationKey, f int64) (model.RebuildStatus, error) {
	return r.call(ctx, k, "activate", 0, f)
}
func (r *remoteRebuild) Abandon(ctx context.Context, k model.GenerationKey, f int64) (model.RebuildStatus, error) {
	return r.call(ctx, k, "abandon", 0, f)
}
