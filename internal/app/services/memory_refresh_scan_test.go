package services

import (
	"context"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

type refreshScanTx struct {
	model.Tx
	versions []model.VersionReference
}

func (t refreshScanTx) MemoryRefreshEvent(context.Context, model.MemoryRefreshTrigger) (model.Event, error) {
	return model.Event{ID: "event", Scope: "source", RecordID: "fact", Type: "supersede_fact", LedgerRevision: 5}, nil
}
func (t refreshScanTx) MemoryRefreshDependents(context.Context, string, string, int64, string, int) ([]model.VersionReference, error) {
	return t.versions, nil
}

type refreshScanLedger struct {
	model.Ledger
	tx refreshScanTx
}

func (l refreshScanLedger) Transact(ctx context.Context, _ string, fn func(model.Tx) error) error {
	return fn(l.tx)
}
func TestRefreshScanDisclosesOnlyAuthorizedDependencies(t *testing.T) {
	c := model.Caller{Tenant: "tenant", Principal: "worker", Service: true}
	q := model.MemoryRefreshScan{Scope: "target", Purpose: "review", Trigger: model.MemoryRefreshTrigger{EventID: "event", LedgerRevision: 5}, Limit: 1}
	versions := []model.VersionReference{{RecordID: "memory", VersionID: "version"}}
	for _, permission := range []string{"can_read", "can_read_history"} {
		s := &Service{ledger: refreshScanLedger{tx: refreshScanTx{versions: versions}}, auth: contextPermissionAuthorizer{denyScope: "source", denyPermission: permission}}
		page, err := s.ScanMemoryRefresh(context.Background(), c, q)
		if !errors.Is(err, model.ErrDenied) || len(page.Versions) != 0 {
			t.Fatal("source dependency identity disclosed", page, err)
		}
		s.ledger = refreshScanLedger{tx: refreshScanTx{}}
		page, err = s.ScanMemoryRefresh(context.Background(), c, q)
		if err != nil || len(page.Versions) != 0 {
			t.Fatal("unrelated source blocked scan", page, err)
		}
	}
}
