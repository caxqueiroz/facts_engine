package postgres_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
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
)

func testRemoteRetention(t *testing.T, ctx context.Context, normal, engine *facts.Engine, ledger *postgres.Ledger, store *evidence.Postgres, caller model.Caller, register func(string, string, string, string) model.ObjectRef) {
	clientFor := func(engine *facts.Engine) memoryv1connect.RetentionServiceClient {
		t.Helper()
		handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return caller, nil }, connectapi.Limits{Timeout: 10 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		return memoryv1connect.NewRetentionServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	}
	if _, err := normal.PutScope(ctx, caller, model.ScopeRequest{Command: model.Command{Key: "remote-retention-scope", Purpose: "remote-retention"}, Definition: model.ScopeDefinition{ID: "remote-retention", Label: "Retention test"}, Reason: "integration"}); err != nil {
		t.Fatal("create retention scope", err)
	}
	client := clientFor(engine)
	grpcClient := v1.NewRetentionServiceClient(governanceGRPC(t, engine, caller))
	command := func(key string) *v1.Command { return &v1.Command{Key: key, Purpose: "remote-retention"} }
	policy := &v1.RegisterRetentionPolicyRequest{Tenant: caller.Tenant, Command: command("remote-policy"), Policy: &v1.RetentionPolicy{Id: "remote-policy", Scope: "remote-retention", RecordClass: "evidence_bytes", Purpose: "remote-retention", Jurisdiction: "SG"}}
	first, err := grpcClient.RegisterRetentionPolicy(ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := client.RegisterRetentionPolicy(ctx, policy)
	if err != nil || !proto.Equal(first, replay) {
		t.Fatal("policy replay", replay, err)
	}
	changed := proto.Clone(policy).(*v1.RegisterRetentionPolicyRequest)
	changed.Policy.MinimumAgeSeconds = 1
	if _, err := client.RegisterRetentionPolicy(ctx, changed); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatal("changed policy replay accepted", err)
	}
	if result, err := grpcClient.RegisterRetentionPolicy(ctx, changed); result != nil || grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatal("gRPC changed policy replay accepted", result, err)
	}
	roleDenied := proto.Clone(policy).(*v1.RegisterRetentionPolicyRequest)
	roleDenied.Command = command("runtime-retention-denied")
	roleDenied.Policy.Id = "runtime-retention-denied"
	if _, err := clientFor(normal).RegisterRetentionPolicy(ctx, roleDenied); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatal("normal database role bypassed", err)
	}
	grpcNormal := v1.NewRetentionServiceClient(governanceGRPC(t, normal, caller))
	if result, err := grpcNormal.RegisterRetentionPolicy(ctx, roleDenied); result != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("gRPC normal database role bypassed", result, err)
	}
	bad := proto.Clone(policy).(*v1.RegisterRetentionPolicyRequest)
	bad.Tenant = "other-tenant"
	if _, err := client.RegisterRetentionPolicy(ctx, bad); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatal("tenant binding", err)
	}
	if result, err := grpcClient.RegisterRetentionPolicy(ctx, bad); result != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("gRPC tenant binding", result, err)
	}
	bad = proto.Clone(policy).(*v1.RegisterRetentionPolicyRequest)
	bad.Policy.MinimumAgeSeconds = -1
	if _, err := client.RegisterRetentionPolicy(ctx, bad); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatal("retention bound", err)
	}
	if result, err := grpcClient.RegisterRetentionPolicy(ctx, bad); result != nil || grpcstatus.Code(err) != codes.InvalidArgument {
		t.Fatal("gRPC retention bound", result, err)
	}
	ref := register("remote-disposable", "remote-disposable-object", "remote-retention", "remote-policy")
	query := &v1.GetRetentionStatusRequest{Tenant: caller.Tenant, Scope: "remote-retention", EvidenceId: "remote-disposable", Purpose: "remote-retention"}
	status, err := client.GetRetentionStatus(ctx, query)
	direct, directErr := engine.GetRetentionStatus(ctx, caller, model.RetentionStatusQuery{Scope: query.Scope, EvidenceID: query.EvidenceId, Purpose: query.Purpose})
	if err != nil || directErr != nil || status.Status.Disposed || !status.Status.RegisteredAt.AsTime().Equal(direct.RegisteredAt) || status.Status.PolicyId != direct.PolicyID {
		t.Fatal("status parity", status, err, directErr)
	}
	hold := &v1.PlaceLegalHoldRequest{Tenant: caller.Tenant, Command: command("remote-hold"), Id: "remote-hold", Scope: query.Scope, Reason: "review"}
	placed, err := grpcClient.PlaceLegalHold(ctx, hold)
	if err != nil {
		t.Fatal(err)
	}
	placedAgain, err := client.PlaceLegalHold(ctx, hold)
	if err != nil || !proto.Equal(placed, placedAgain) {
		t.Fatal("hold replay", err)
	}
	status, err = client.GetRetentionStatus(ctx, query)
	if err != nil || len(status.Status.ActiveHolds) != 1 || status.Status.ActiveHolds[0] != hold.Id {
		t.Fatal("hold status", status, err)
	}
	grpcStatus, err := grpcClient.GetRetentionStatus(ctx, query)
	if err != nil || !proto.Equal(status, grpcStatus) {
		t.Fatal("gRPC held status differs", grpcStatus, err)
	}
	disposal := &v1.DisposeEvidenceRequest{Tenant: caller.Tenant, Command: command("remote-disposal"), Scope: query.Scope, EvidenceId: query.EvidenceId, PolicyId: "remote-policy", Jurisdiction: "SG", Reason: "expired"}
	if _, err := client.DisposeEvidence(ctx, disposal); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatal("hold bypassed remotely", err)
	}
	if result, err := grpcClient.DisposeEvidence(ctx, disposal); result != nil || grpcstatus.Code(err) != codes.FailedPrecondition {
		t.Fatal("gRPC hold bypassed", result, err)
	}
	denied, err := facts.New(ledger, denyPermission{permission: "can_manage_legal_hold"}, store)
	if err != nil {
		t.Fatal(err)
	}
	release := &v1.ReleaseLegalHoldRequest{Tenant: caller.Tenant, Command: command("remote-release"), Id: hold.Id, Scope: hold.Scope, Reason: "review-finished"}
	if _, err := clientFor(denied).ReleaseLegalHold(ctx, release); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatal("hold permission bypassed", err)
	}
	grpcDenied := v1.NewRetentionServiceClient(governanceGRPC(t, denied, caller))
	if result, err := grpcDenied.ReleaseLegalHold(ctx, release); result != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("gRPC hold permission bypassed", result, err)
	}
	released, err := grpcClient.ReleaseLegalHold(ctx, release)
	if err != nil {
		t.Fatal(err)
	}
	releasedAgain, err := client.ReleaseLegalHold(ctx, release)
	if err != nil || !proto.Equal(released, releasedAgain) {
		t.Fatal("release replay", err)
	}
	disposed, err := grpcClient.DisposeEvidence(ctx, disposal)
	if err != nil {
		t.Fatal(err)
	}
	disposedAgain, err := client.DisposeEvidence(ctx, disposal)
	if err != nil || !proto.Equal(disposed, disposedAgain) {
		t.Fatal("disposal replay", err)
	}
	status, err = client.GetRetentionStatus(ctx, query)
	if err != nil || !status.Status.Disposed || len(status.Status.ActiveHolds) != 0 {
		t.Fatal(status, err)
	}
	grpcStatus, err = grpcClient.GetRetentionStatus(ctx, query)
	if err != nil || !proto.Equal(status, grpcStatus) {
		t.Fatal("gRPC disposed status differs", grpcStatus, err)
	}
	if _, err := store.Read(ctx, caller.Tenant, ref); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("disposed bytes retained", err)
	}
	register("remote-shared-a", "remote-shared-object", query.Scope, "remote-policy")
	register("remote-shared-b", "remote-shared-object", query.Scope, "remote-policy")
	disposal = proto.Clone(disposal).(*v1.DisposeEvidenceRequest)
	disposal.Command = command("remote-shared-disposal")
	disposal.EvidenceId = "remote-shared-a"
	if _, err := client.DisposeEvidence(ctx, disposal); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatal("shared-object blocker bypassed", err)
	}
	if result, err := grpcClient.DisposeEvidence(ctx, disposal); result != nil || grpcstatus.Code(err) != codes.FailedPrecondition {
		t.Fatal("gRPC shared-object blocker bypassed", result, err)
	}
}
