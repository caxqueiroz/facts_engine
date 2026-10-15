package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type interruptedEvidenceStore struct {
	model.EvidenceStore
	err error
}

func (s interruptedEvidenceStore) Read(context.Context, string, model.ObjectRef) ([]byte, error) {
	return nil, fmt.Errorf("PRIVATE evidence error: %w", s.err)
}
func testEvidenceInterruption(t *testing.T, ctx context.Context, l model.Ledger, store model.EvidenceStore, c model.Caller, key model.FactKey, at time.Time) {
	t.Helper()
	revision := func() int64 {
		t.Helper()
		var n int64
		err := l.Transact(ctx, c.Tenant, func(tx model.Tx) error { var err error; n, err = tx.Revision(ctx); return err })
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := revision()
	for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
		broken, err := facts.NewWithReads(l, allow{}, interruptedEvidenceStore{store, want}, model.ReadOptions{PaginationKey: bytes.Repeat([]byte{33}, 32)})
		if err != nil {
			t.Fatal(err)
		}
		got, err := broken.GetFact(ctx, c, model.FactQuery{Key: key, ValidAt: at, Purpose: "interruption-test"})
		if !errors.Is(err, want) || got.State != "" || got.Value != nil {
			t.Fatal("read became ineligible", got, err)
		}
		page, err := broken.ListFacts(ctx, c, model.FactsQuery{Scope: key.Scope, Subject: key.Subject, Predicate: key.Predicate, ValidAt: at, Purpose: "interruption-test", Limit: 10})
		if !errors.Is(err, want) || len(page.Items) != 0 {
			t.Fatal("list returned partial results", page, err)
		}
		snapshot, err := broken.CreateFactSnapshot(ctx, c, model.FactSnapshotRequest{Command: model.Command{Key: "interrupted-snapshot", Purpose: "interruption-test"}, Selectors: []model.FactKey{key}, ValidAt: at})
		if !errors.Is(err, want) || snapshot.ID != "" || len(snapshot.Items) != 0 {
			t.Fatal("snapshot persisted interruption", snapshot, err)
		}
		rpc := v1.NewFactServiceClient(governanceGRPC(t, broken, c))
		remote, err := rpc.GetFact(ctx, &v1.GetFactRequest{Tenant: c.Tenant, Purpose: "interruption-test", Key: &v1.FactKey{Scope: key.Scope, Subject: key.Subject, Predicate: key.Predicate, Discriminator: key.Discriminator}, ValidAt: timestamppb.New(at)})
		code := codes.Canceled
		if want == context.DeadlineExceeded {
			code = codes.DeadlineExceeded
		}
		if status.Code(err) != code || remote != nil {
			t.Fatal("RPC interruption changed", remote, err)
		}
	}
	if revision() != before {
		t.Fatal("interrupted reads appended ledger state")
	}
}
