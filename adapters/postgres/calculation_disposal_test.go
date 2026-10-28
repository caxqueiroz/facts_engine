package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testCalculationDisposal(t *testing.T, ctx context.Context, normal, engine *facts.Engine, ledger *postgres.Ledger, admin *pgxpool.Pool, c model.Caller, base model.CalculationReceipt, snapshots []model.FactSnapshot) string {
	t.Helper()
	cmd := func(key string) model.Command {
		return model.Command{Key: "calc-disposal-" + key, Purpose: "snapshot-retention"}
	}
	for _, scope := range []string{"case1", "case2"} {
		for _, young := range []bool{false, true} {
			p := model.RetentionPolicy{ID: "calculations", Scope: scope, RecordClass: "calculation_payload", Purpose: "snapshot-retention", Jurisdiction: "XX"}
			if young {
				p.ID = "calculations-young"
				p.MinimumAgeSeconds = 3600
			}
			if _, err := engine.RegisterRetentionPolicy(ctx, c, model.RetentionPolicyRequest{Command: cmd(scope + p.ID), Policy: p}); err != nil {
				t.Fatal(err)
			}
			p.RecordClass = "decision_payload"
			p.ID = "decisions"
			if young {
				p.ID = "decisions-young"
			}
			if _, err := engine.RegisterRetentionPolicy(ctx, c, model.RetentionPolicyRequest{Command: cmd(scope + p.ID), Policy: p}); err != nil {
				t.Fatal(err)
			}
		}
	}
	reject := func(e *facts.Engine, r model.DisposalBatchRequest, want error) {
		t.Helper()
		if _, err := e.DisposeBatch(ctx, c, r); !errors.Is(err, want) {
			t.Fatal("calculation batch rejection", err, want)
		}
	}
	single := ""
	for _, snapshot := range snapshots {
		receipt := base
		receipt.Command = cmd("record-" + snapshot.ID)
		receipt.ExternalRunID = receipt.Key
		receipt.SnapshotID = snapshot.ID
		calc, err := normal.RecordCalculation(ctx, c, receipt)
		if err != nil {
			t.Fatal(err)
		}
		if single == "" {
			single = calc.ID
		}
		selection := model.CalculationDisposalRequest{CalculationID: calc.ID}
		snapSelection := model.SnapshotDisposalRequest{SnapshotID: snapshot.ID}
		for _, item := range snapshot.Items {
			selection.Bindings = append(selection.Bindings, model.CalculationRetentionBinding{Scope: item.Selector.Scope, PolicyID: "calculations", Jurisdiction: "XX"})
			snapSelection.Bindings = append(snapSelection.Bindings, model.SnapshotRetentionBinding{Scope: item.Selector.Scope, PolicyID: "snapshots", Jurisdiction: "XX"})
		}
		r := model.DisposalBatchRequest{Command: cmd("dispose-" + calc.ID), Scope: "case1", Reason: "expired", Calculations: []model.CalculationDisposalRequest{selection}, Snapshots: []model.SnapshotDisposalRequest{snapSelection}}
		used := []model.VersionReference{}
		for _, item := range snapshot.Items {
			used = append(used, model.VersionReference{RecordID: item.Resolution.RecordID, VersionID: item.Resolution.VersionID})
		}
		decision, err := normal.RecordDecision(ctx, c, model.DecisionReceipt{Command: cmd("decision-" + calc.ID), Scope: "case1", ApplicationID: "batch", ExternalDecisionID: calc.ID, SnapshotID: snapshot.ID, CalculationID: calc.ID, SelectedItems: used, UsedItems: used, MethodKind: "rule", MethodID: "rule", MethodVersion: "v1", TransformationVersion: "v1", Input: receipt.Input, Output: receipt.Output, DecidedAt: time.Now().UTC(), Outcome: "succeeded"})
		if err != nil {
			t.Fatal(err)
		}
		ds := model.DecisionDisposalRequest{DecisionID: decision.ID}
		for _, scope := range decision.Scopes {
			ds.Bindings = append(ds.Bindings, model.DecisionRetentionBinding{Scope: scope, PolicyID: "decisions", Jurisdiction: "XX"})
		}
		r.Decisions = []model.DecisionDisposalRequest{ds}
		retained := func() {
			t.Helper()
			if _, err := normal.GetDecisionReceipt(ctx, c, model.DecisionQuery{ID: decision.ID, Purpose: "read"}); err != nil {
				t.Fatal("failed batch lost decision", err)
			}
			if _, err := normal.GetCalculationReceipt(ctx, c, model.CalculationQuery{ID: calc.ID, Purpose: "read"}); err != nil {
				t.Fatal("failed batch lost calculation", err)
			}
			if _, err := normal.GetFactSnapshot(ctx, c, model.SnapshotQuery{ID: snapshot.ID, Purpose: "read"}); err != nil {
				t.Fatal("failed batch lost snapshot", err)
			}
		}
		if _, err := admin.Exec(ctx, "DELETE FROM decision_items WHERE tenant_id=$1 AND decision_id=$2 AND role='selected' AND ordinal=0", c.Tenant, decision.ID); err != nil {
			t.Fatal(err)
		}
		reject(engine, r, model.ErrPrecondition)
		if _, err := admin.Exec(ctx, "INSERT INTO decision_items(tenant_id,decision_id,role,ordinal,record_id,version_id,scope_id) VALUES($1,$2,'selected',0,$3,$4,$5)", c.Tenant, decision.ID, used[0].RecordID, used[0].VersionID, snapshot.Items[0].Selector.Scope); err != nil {
			t.Fatal(err)
		}
		retained()
		if _, err := admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,'archive-decision-pending',3)", c.Tenant); err != nil {
			t.Fatal(err)
		}
		reject(engine, r, model.ErrPrecondition)
		if _, err := admin.Exec(ctx, "DELETE FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id='archive-decision-pending'", c.Tenant); err != nil {
			t.Fatal(err)
		}
		noDecision := r
		noDecision.Decisions = nil
		reject(engine, noDecision, model.ErrPrecondition)
		youngDecision := r
		youngDecision.Decisions = append([]model.DecisionDisposalRequest(nil), r.Decisions...)
		youngDecision.Decisions[0].Bindings = append([]model.DecisionRetentionBinding(nil), ds.Bindings...)
		youngDecision.Decisions[0].Bindings[0].PolicyID = "decisions-young"
		reject(engine, youngDecision, model.ErrPrecondition)
		if len(ds.Bindings) > 1 {
			missing := r
			missing.Decisions = append([]model.DecisionDisposalRequest(nil), r.Decisions...)
			missing.Decisions[0].Bindings = ds.Bindings[:1]
			reject(engine, missing, model.ErrDenied)
		}
		if _, err := admin.Exec(ctx, "REVOKE DELETE ON decision_payloads FROM facts_retention"); err != nil {
			t.Fatal(err)
		}
		_, decisionErr := engine.DisposeBatch(ctx, c, r)
		if _, err := admin.Exec(ctx, "GRANT DELETE ON decision_payloads TO facts_retention"); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(decisionErr, model.ErrDenied) {
			t.Fatal(decisionErr)
		}
		retained()
		reject(normal, r, model.ErrDenied)
		incomplete := r
		incomplete.Calculations = nil
		reject(engine, incomplete, model.ErrPrecondition)
		bad := r
		bad.Snapshots = append([]model.SnapshotDisposalRequest(nil), r.Snapshots...)
		bad.Snapshots[0].Bindings = append([]model.SnapshotRetentionBinding(nil), snapSelection.Bindings...)
		bad.Snapshots[0].Bindings[0].PolicyID = "young"
		reject(engine, bad, model.ErrPrecondition)
		retained()
		bad = r
		bad.Calculations = append([]model.CalculationDisposalRequest(nil), r.Calculations...)
		bad.Calculations[0].Bindings = append([]model.CalculationRetentionBinding(nil), selection.Bindings...)
		bad.Calculations[0].Bindings[0].PolicyID = "calculations-young"
		reject(engine, bad, model.ErrPrecondition)
		if len(selection.Bindings) > 1 {
			bad = r
			bad.Calculations = append([]model.CalculationDisposalRequest(nil), r.Calculations...)
			bad.Calculations[0].Bindings = selection.Bindings[:1]
			reject(engine, bad, model.ErrDenied)
			hold := model.LegalHoldRequest{Command: cmd("hold"), ID: "calc-hold", Scope: "case2", Reason: "review"}
			if _, err := engine.PlaceLegalHold(ctx, c, hold); err != nil {
				t.Fatal(err)
			}
			reject(engine, r, model.ErrPrecondition)
			if _, err := engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: cmd("release"), ID: hold.ID, Scope: hold.Scope, Reason: "released"}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := admin.Exec(ctx, "REVOKE DELETE ON calculation_payloads FROM facts_retention"); err != nil {
			t.Fatal(err)
		}
		_, disposeErr := engine.DisposeBatch(ctx, c, r)
		if _, err := admin.Exec(ctx, "GRANT DELETE ON calculation_payloads TO facts_retention"); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(disposeErr, model.ErrDenied) {
			t.Fatal("missing privilege", disposeErr)
		}
		retained()
		first, err := engine.DisposeBatch(ctx, c, r)
		if err != nil {
			t.Fatal("complete calculation selection", err)
		}
		testDisposalBatchRemote(t, ctx, engine, c, r, first)
		if _, err := normal.GetDecisionReceipt(ctx, c, model.DecisionQuery{ID: decision.ID, Purpose: "read"}); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("decision retained", err)
		}
		if _, err := normal.GetCalculationReceipt(ctx, c, model.CalculationQuery{ID: calc.ID, Purpose: "read"}); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("calculation retained", err)
		}
		if _, err := normal.GetFactSnapshot(ctx, c, model.SnapshotQuery{ID: snapshot.ID, Purpose: "read"}); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("snapshot retained", err)
		}
		changes, err := normal.ListChanges(ctx, c, model.ChangesQuery{Scope: "case1", Purpose: "archive", Limit: 1000})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, event := range changes.Events {
			if event.LedgerRevision == calc.LedgerRevision {
				payload, err := ledger.ArchivePayload(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant, Consumer: "archive-calculation"}, Event: event})
				if err != nil {
					t.Fatal(err)
				}
				for _, gap := range payload.Gaps {
					if gap.Kind == "calculation_content" && gap.ID == calc.ID {
						found = true
					}
				}
			}
		}
		if !found {
			t.Fatal("calculation archive gap missing")
		}
	}
	// A retained decision blocks its calculation independently of selected snapshots.
	receipt := base
	receipt.Command = cmd("decision-calculation")
	receipt.ExternalRunID = receipt.Key
	calc, err := normal.RecordCalculation(ctx, c, receipt)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := normal.GetFactSnapshot(ctx, c, model.SnapshotQuery{ID: receipt.SnapshotID, Purpose: "read"})
	if err != nil {
		t.Fatal(err)
	}
	used := model.VersionReference{RecordID: snapshot.Items[0].Resolution.RecordID, VersionID: snapshot.Items[0].Resolution.VersionID}
	if _, err = normal.RecordDecision(ctx, c, model.DecisionReceipt{Command: cmd("decision"), Scope: "case1", ApplicationID: "app", ExternalDecisionID: "calculation-retention", SnapshotID: snapshot.ID, CalculationID: calc.ID, SelectedItems: []model.VersionReference{used}, UsedItems: []model.VersionReference{used}, MethodKind: "rule", MethodID: "rule", MethodVersion: "v1", TransformationVersion: "v1", Input: receipt.Input, Output: receipt.Output, DecidedAt: time.Now().UTC(), Outcome: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	reject(engine, model.DisposalBatchRequest{Command: cmd("blocked-decision"), Scope: "case1", Reason: "expired", Calculations: []model.CalculationDisposalRequest{{CalculationID: calc.ID, Bindings: []model.CalculationRetentionBinding{{Scope: "case1", PolicyID: "calculations", Jurisdiction: "XX"}}}}}, model.ErrPrecondition)
	return single
}
