package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
)

type receiptCheckTx struct {
	model.Tx
	receipt *model.CommitResult
}

func (t *receiptCheckTx) Receipt(context.Context, model.Caller, string, string, string) (*model.CommitResult, error) {
	return t.receipt, nil
}
func (t *receiptCheckTx) Append(_ context.Context, e model.Event) (model.Event, error) {
	e.LedgerRevision = 1
	return e, nil
}
func (t *receiptCheckTx) SaveReceipt(_ context.Context, _ model.Caller, _, _, _ string, r model.CommitResult) error {
	t.receipt = &r
	return nil
}

type receiptCheckLedger struct{ tx *receiptCheckTx }

func (l receiptCheckLedger) Transact(_ context.Context, _ string, fn func(model.Tx) error) error {
	// Model discovering a concurrent commit on a later transaction attempt.
	if err := fn(l.tx); err != nil {
		return err
	}
	return fn(l.tx)
}
func TestMutationChecksDistinguishReceiptFromNewAcceptance(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		tx := &receiptCheckTx{}
		s := &Service{ledger: receiptCheckLedger{tx}, auth: waitAuth{check: func() bool { return true }}}
		checks := 0
		expired := func() error {
			checks++
			if checks > 1 {
				return model.ErrPrecondition
			}
			return nil
		}
		guard := commitOnlyCheck(expired)
		if fresh {
			guard = freshnessCheck(expired)
		}
		result, err := s.mutate(context.Background(), model.Caller{Tenant: "tenant", Principal: "caller"}, "scope", "put_scope", "can_manage_identities", model.Command{Key: "command", Purpose: "test"}, nil, func(_ model.Tx, r *model.CommitResult) error { r.ID = "result"; return nil }, guard)
		if fresh {
			if !errors.Is(err, model.ErrPrecondition) || checks != 2 {
				t.Fatal("stale identity receipt disclosed", err, checks)
			}
		} else if err != nil || checks != 1 || result.ID != "result" {
			t.Fatal("stored outcome re-evaluated as new acceptance", result, err, checks)
		}
	}
}
