package postgres_test

import (
	"connectrpc.com/connect/v2"
	"context"
	facts "github.com/caxqueiroz/facts_engine"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

func testRemoteRecall(t *testing.T, ctx context.Context, engine *facts.Engine, caller model.Caller, q model.RecallRequest, code connect.Code) {
	t.Helper()
	transport := governanceClient(t, engine, caller)
	client := memoryv1connect.NewRetrievalServiceClient(transport)
	grpcClient := v1.NewRetrievalServiceClient(governanceGRPC(t, engine, caller))
	query := &v1.RecallQuery{Scopes: q.Scopes, Query: q.Query, Purpose: q.Purpose, ValidAt: timestamppb.New(q.ValidAt), KnowledgeRevision: q.KnowledgeRevision, RequireRevision: q.RequireRevision, RequireGeneration: q.RequireGeneration, Limit: int32(q.Limit)}
	remote, err := client.Recall(ctx, &v1.RecallRequest{Tenant: caller.Tenant, Query: query})
	grpcRemote, grpcErr := grpcClient.Recall(ctx, &v1.RecallRequest{Tenant: caller.Tenant, Query: query})
	if code != 0 {
		if grpcRemote != nil || status.Code(grpcErr) != codes.Code(code) {
			t.Fatal("gRPC recall status", grpcRemote, grpcErr)
		}
		if connect.CodeOf(err) != code {
			t.Fatalf("recall status: %v", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if grpcErr != nil || !proto.Equal(remote, grpcRemote) {
		t.Fatal("gRPC recall coverage and results differ", grpcRemote, grpcErr)
	}
	direct, err := engine.Recall(ctx, caller, q)
	if err != nil || remote.Generation != direct.Generation || remote.LedgerRevision != direct.LedgerRevision || remote.IndexedRevision != direct.IndexedRevision || remote.Incomplete != direct.Incomplete || len(remote.Items) != len(direct.Items) {
		t.Fatalf("recall coverage: %v, %+v, %v", remote, direct, err)
	}
	reads := memoryv1connect.NewFactServiceClient(transport)
	for i, item := range remote.Items {
		expected := direct.Items[i]
		if item.Key.Scope != expected.Key.Scope || item.Key.Subject != expected.Key.Subject || item.Key.Predicate != expected.Key.Predicate || item.Key.Discriminator != expected.Key.Discriminator || item.Score != expected.Score || item.Fact.VersionId != expected.Fact.VersionID {
			t.Fatal("recall ordering/identity mismatch")
		}
		read, err := reads.GetFact(ctx, &v1.GetFactRequest{Tenant: caller.Tenant, Key: item.Key, Purpose: q.Purpose, ValidAt: query.ValidAt, KnowledgeRevision: q.KnowledgeRevision})
		if err != nil || !proto.Equal(read.Result, item.Fact) {
			t.Fatalf("recall differs from authoritative read: %v", err)
		}
	}
	memories, err := client.RecallMemories(ctx, &v1.RecallMemoriesRequest{Tenant: caller.Tenant, Query: query})
	if err != nil {
		t.Fatal(err)
	}
	grpcMemories, err := grpcClient.RecallMemories(ctx, &v1.RecallMemoriesRequest{Tenant: caller.Tenant, Query: query})
	if err != nil || !proto.Equal(memories, grpcMemories) {
		t.Fatal("gRPC memory recall differs", grpcMemories, err)
	}
	expected, err := engine.RecallMemories(ctx, caller, q)
	if err != nil || memories.Generation != expected.Generation || memories.LedgerRevision != expected.LedgerRevision || memories.IndexedRevision != expected.IndexedRevision || memories.Incomplete != expected.Incomplete || len(memories.Items) != len(expected.Items) {
		t.Fatalf("memory coverage: %v", err)
	}
	memoryReads := memoryv1connect.NewMemoryServiceClient(transport)
	for i, item := range memories.Items {
		if item.Score != expected.Items[i].Score || item.Memory.VersionId != expected.Items[i].Memory.VersionID {
			t.Fatal("memory ranking mismatch")
		}
		read, err := memoryReads.GetMemory(ctx, &v1.GetMemoryRequest{Tenant: caller.Tenant, Key: item.Key, Purpose: q.Purpose, ValidAt: query.ValidAt, KnowledgeRevision: q.KnowledgeRevision})
		if err != nil || !proto.Equal(read.Result, item.Memory) {
			t.Fatalf("memory recall differs from authoritative read: %v", err)
		}
	}
	if direct.IndexedRevision > 0 {
		wait := &v1.WaitForProjectionRequest{Tenant: caller.Tenant, Scopes: q.Scopes, Generation: direct.Generation, Purpose: q.Purpose, Revision: direct.IndexedRevision}
		receipt, err := client.WaitForProjection(ctx, wait)
		if err != nil || receipt.Generation != direct.Generation || receipt.IndexedRevision < wait.Revision {
			t.Fatalf("projection receipt: %v, %v", receipt, err)
		}
		grpcReceipt, err := grpcClient.WaitForProjection(ctx, wait)
		if err != nil || !proto.Equal(receipt, grpcReceipt) {
			t.Fatal("gRPC projection receipt differs", grpcReceipt, err)
		}
		wait.Generation = "missing-generation"
		if _, err := client.WaitForProjection(ctx, wait); connect.CodeOf(err) != connect.CodeUnavailable {
			t.Fatalf("wait generation: %v", err)
		}
		if result, err := grpcClient.WaitForProjection(ctx, wait); result != nil || status.Code(err) != codes.Unavailable {
			t.Fatal("gRPC wait generation", result, err)
		}
		wait.Generation = direct.Generation
		wait.Revision = direct.LedgerRevision + 1000000
		bounded, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		if _, err := client.WaitForProjection(bounded, wait); connect.CodeOf(err) != connect.CodeDeadlineExceeded {
			t.Fatalf("wait deadline: %v", err)
		}
		grpcBounded, grpcCancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer grpcCancel()
		if result, err := grpcClient.WaitForProjection(grpcBounded, wait); result != nil || status.Code(err) != codes.DeadlineExceeded {
			t.Fatal("gRPC wait deadline", result, err)
		}
	}
}
