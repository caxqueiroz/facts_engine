package postgres_test

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"testing"
)

// pauseLedger suspends the first record lookup after its transaction snapshot is
// established, allowing the integration test to commit a concurrent fact change.
type pauseLedger struct {
	model.Ledger
	once          sync.Once
	read, proceed chan struct{}
}
type pauseTx struct {
	model.TemporalTx
	parent *pauseLedger
}

func (l *pauseLedger) Transact(ctx context.Context, tenant string, fn func(model.Tx) error) error {
	return l.Ledger.Transact(ctx, tenant, func(tx model.Tx) error { return fn(&pauseTx{tx.(model.TemporalTx), l}) })
}
func (t *pauseTx) Record(ctx context.Context, id string) (model.Record, error) {
	r, err := t.TemporalTx.Record(ctx, id)
	t.parent.once.Do(func() {
		close(t.parent.read)
		select {
		case <-t.parent.proceed:
		case <-ctx.Done():
		}
	})
	return r, err
}

type denyScope struct{ scope string }

func (d denyScope) Check(ctx context.Context, r model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	decision, err := (allow{}).Check(ctx, r)
	if r.Scope == d.scope {
		decision.Allowed = false
	}
	return decision, err
}

// The migration treats encrypted payloads as opaque bytes; they must survive
// unchanged while cumulative historical generations are reconstructed.
func seedLegacyTimeline(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	t.Helper()
	_, err := db.Exec(ctx, `
 INSERT INTO tenant_revisions VALUES('legacy',2);
 INSERT INTO policy_versions VALUES('legacy','scope','policy',decode('01','hex'));
 INSERT INTO predicate_versions VALUES('legacy','scope','predicate','v1','policy',decode('01','hex'));
 INSERT INTO records VALUES('legacy','record','scope','subject','predicate','',4);
 INSERT INTO record_versions VALUES('legacy','record','a','scope','predicate','v1','digest-a',decode('01','hex')),('legacy','record','b','scope','predicate','v1','digest-b',decode('02','hex'));
 INSERT INTO ledger_events(tenant_id,id,ledger_revision,event_type,scope_id,payload) VALUES('legacy','ev1',1,'approve_fact','scope',decode('01','hex')),('legacy','ev2',2,'approve_fact','scope',decode('02','hex'));
 INSERT INTO accepted_intervals VALUES('legacy','record','a','2026-01-01','2026-02-01',1,decode('01','hex')),('legacy','record','b','2026-02-01',NULL,2,decode('02','hex'));
 `)
	if err != nil {
		t.Fatal(err)
	}
}
