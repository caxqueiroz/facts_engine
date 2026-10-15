package postgres_test

import (
	"context"
	"testing"

	"connectrpc.com/connect/v2"
	facts "github.com/caxqueiroz/facts_engine"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func testRemoteConversationDisposal(t *testing.T, ctx context.Context, engine *facts.Engine, caller model.Caller, request model.ConversationDisposalRequest) {
	t.Helper()
	wire := &v1.DisposeConversationRequest{
		Tenant:  caller.Tenant,
		Command: &v1.Command{Key: request.Key, Purpose: request.Purpose, CorrelationId: request.CorrelationID},
		Scope:   request.Scope, ConversationId: request.ConversationID, PolicyId: request.PolicyID,
		Jurisdiction: request.Jurisdiction, Purpose: request.Purpose, Reason: request.Reason,
	}
	grpcClient := v1.NewRetentionServiceClient(governanceGRPC(t, engine, caller))
	grpcResult, err := grpcClient.DisposeConversation(ctx, wire)
	if err != nil {
		t.Fatal("gRPC conversation disposal", err)
	}
	connectClient := memoryv1connect.NewRetentionServiceClient(governanceClient(t, engine, caller))
	connectResult, err := connectClient.DisposeConversation(ctx, wire)
	if err != nil || !proto.Equal(grpcResult, connectResult) {
		t.Fatal("Connect conversation disposal differs", connectResult, err)
	}
	direct, err := engine.DisposeConversation(ctx, caller, request)
	if err != nil {
		t.Fatal("embedded conversation disposal replay", err)
	}
	want := &v1.CommitResult{Id: direct.ID, RecordId: direct.RecordID, VersionId: direct.VersionID, RecordKind: direct.RecordKind, ContentDigest: direct.ContentDigest, StreamRevision: direct.StreamRevision, LedgerRevision: direct.LedgerRevision}
	if !proto.Equal(grpcResult.Result, want) {
		t.Fatal("conversation disposal receipt differs from embedded result", grpcResult.Result, want)
	}
	changed := proto.Clone(wire).(*v1.DisposeConversationRequest)
	changed.Reason = "changed retry"
	if result, err := grpcClient.DisposeConversation(ctx, changed); result != nil || grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatal("gRPC changed command retry was accepted", result, err)
	}
	if result, err := connectClient.DisposeConversation(ctx, changed); result != nil || connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatal("Connect changed command retry was accepted", result, err)
	}
}
