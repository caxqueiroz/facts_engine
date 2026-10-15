package postgres_test

import (
	"context"
	"fmt"
	"testing"

	facts "github.com/caxqueiroz/facts_engine"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type interruptedRecallIndex struct {
	model.RetrievalIndex
	err error
}

func (i interruptedRecallIndex) Candidates(context.Context, string, string, string, string, int) ([]model.SearchCandidate, error) {
	return nil, fmt.Errorf("PRIVATE index: %w", i.err)
}

func testRetrievalInterruption(t *testing.T, ctx context.Context, ledger model.Ledger, evidence model.EvidenceStore, progress model.Outbox, index model.RetrievalIndex, caller model.Caller, request model.ContextRequest) {
	t.Helper()
	for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
		engine, err := facts.NewWithRetrieval(ledger, allow{}, evidence, model.RetrievalOptions{Index: interruptedRecallIndex{index, want}, Progress: progress, Generation: "g1"})
		if err != nil {
			t.Fatal(err)
		}
		result, err := engine.Recall(ctx, caller, request.Query)
		if err != want || len(result.Items) != 0 {
			t.Fatal("fact recall interruption", result, err)
		}
		memories, err := engine.RecallMemories(ctx, caller, request.Query)
		if err != want || len(memories.Items) != 0 {
			t.Fatal("memory recall interruption", memories, err)
		}
		for _, mixed := range []bool{false, true} {
			q := request
			q.Key = "interrupted-index-context"
			q.IncludeMemories = mixed
			manifest, err := engine.PrepareContext(ctx, caller, q)
			if err != want || manifest.ID != "" || len(manifest.Rendered) != 0 {
				t.Fatal("context index interruption", manifest, err)
			}
		}
		client := v1.NewRetrievalServiceClient(governanceGRPC(t, engine, caller))
		query := contextRequestProto(caller, request).Context.Query
		remote, err := client.Recall(ctx, &v1.RecallRequest{Tenant: caller.Tenant, Query: query})
		code := codes.Canceled
		if want == context.DeadlineExceeded {
			code = codes.DeadlineExceeded
		}
		if remote != nil || status.Code(err) != code || status.Convert(err).Message() != want.Error() {
			t.Fatal("gRPC index interruption", remote, err)
		}
	}
}
