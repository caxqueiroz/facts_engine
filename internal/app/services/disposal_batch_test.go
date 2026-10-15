package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func (t *extractionDisposalTestTx) DisposeBatch(ctx context.Context, r model.DisposalBatchRequest) (model.DisposalBatchResult, error) {
	err := t.DisposeExtraction(ctx, model.ExtractionDisposalRequest{})
	revisions := make([]int64, len(r.Versions))
	for i := range revisions {
		revisions[i] = int64(i + 2)
	}
	return model.DisposalBatchResult{VersionRevisions: revisions, AttestationRevisions: make([]int64, len(r.Attestations))}, err
}
func TestDisposalBatchReauthorizesAndNormalizesSelection(t *testing.T) {
	for _, phase := range []string{"success", "before", "after-disposal", "after-child"} {
		t.Run(phase, func(t *testing.T) {
			s, l, a, c, e := extractionDisposalFixture()
			e.Command = model.Command{}
			e.Reason = ""
			r := model.DisposalBatchRequest{Command: model.Command{Key: "batch", Purpose: "retention"}, Scope: "z", Reason: "expired", Extractions: []model.ExtractionDisposalRequest{e}}
			for _, kind := range []string{"fact", "memory"} {
				r.Versions = append(r.Versions, model.VersionDisposalRequest{Kind: kind, Scope: "z", RecordID: kind + "-record", VersionID: kind + "-version", Bindings: []model.VersionRetentionBinding{{Scope: "z", PolicyID: "policy", Jurisdiction: "SG"}, {Scope: "a", PolicyID: "policy", Jurisdiction: "SG"}}})
			}
			r.Snapshots = []model.SnapshotDisposalRequest{{SnapshotID: "snapshot", Bindings: []model.SnapshotRetentionBinding{{Scope: "a", PolicyID: "policy", Jurisdiction: "SG"}}}}
			r.Contexts = []model.ContextDisposalRequest{{ContextID: "context", Bindings: []model.ContextRetentionBinding{{Scope: "z", PolicyID: "policy", Jurisdiction: "SG"}}}}
			r.Calculations = []model.CalculationDisposalRequest{{CalculationID: "calculation", Bindings: []model.CalculationRetentionBinding{{Scope: "a", PolicyID: "policy", Jurisdiction: "SG"}}}}
			r.Decisions = []model.DecisionDisposalRequest{{DecisionID: "decision", Bindings: []model.DecisionRetentionBinding{{Scope: "a", PolicyID: "policy", Jurisdiction: "SG"}}}}
			revoke := func() { a.denied = "a" }
			switch phase {
			case "before":
				revoke()
			case "after-disposal":
				l.tx.afterDispose = revoke
			case "after-child":
				l.tx.afterAppend = revoke
			}
			first, err := s.DisposeBatch(context.Background(), c, r)
			if phase != "success" {
				if !errors.Is(err, model.ErrDenied) || l.tx.disposed != 0 || l.tx.receipt != nil {
					t.Fatal(first, err, l.tx)
				}
				return
			}
			if err != nil || l.tx.event.DisposalBatch == nil || l.tx.event.Kind != "dispose_batch" || l.tx.event.ID != first.ID {
				t.Fatal(first, err, l.tx.event)
			}
			r.Versions[0], r.Versions[1] = r.Versions[1], r.Versions[0]
			r.Extractions[0].Bindings[0], r.Extractions[0].Bindings[1] = r.Extractions[0].Bindings[1], r.Extractions[0].Bindings[0]
			for i := range r.Versions {
				r.Versions[i].Bindings[0], r.Versions[i].Bindings[1] = r.Versions[i].Bindings[1], r.Versions[i].Bindings[0]
			}
			replay, err := s.DisposeBatch(context.Background(), c, r)
			if err != nil || replay != first || l.tx.disposed != 1 {
				t.Fatal(replay, err, l.tx.disposed)
			}
			r.Reason = "changed"
			if _, err = s.DisposeBatch(context.Background(), c, r); !errors.Is(err, model.ErrConflict) {
				t.Fatal(err)
			}
		})
	}
}
func TestDisposalBatchRejectsAmbiguousSelections(t *testing.T) {
	base := model.DisposalBatchRequest{Command: model.Command{Key: "batch", Purpose: "retention"}, Scope: "scope", Reason: "expired", Versions: []model.VersionDisposalRequest{{Kind: "fact", Scope: "scope", RecordID: "record", VersionID: "version", Bindings: []model.VersionRetentionBinding{{Scope: "scope", PolicyID: "policy", Jurisdiction: "SG"}}}}}
	for _, change := range []func(*model.DisposalBatchRequest){
		func(r *model.DisposalBatchRequest) { r.Versions = nil },
		func(r *model.DisposalBatchRequest) { r.Versions = append(r.Versions, r.Versions[0]) },
		func(r *model.DisposalBatchRequest) { r.Versions[0].Command = model.Command{Key: "different"} },
		func(r *model.DisposalBatchRequest) { r.Versions[0].Reason = "different" },
		func(r *model.DisposalBatchRequest) { r.Scope = "unbound" },
	} {
		r := base
		r.Versions = append([]model.VersionDisposalRequest(nil), base.Versions...)
		change(&r)
		s, _, a, c, _ := extractionDisposalFixture()
		if _, err := s.DisposeBatch(context.Background(), c, r); !errors.Is(err, model.ErrInvalid) || len(a.requests) != 0 {
			t.Fatal("invalid batch authorized", r, err)
		}
	}
	raw, err := json.Marshal(model.RetentionEvent{Kind: "dispose_batch", DisposalBatch: &model.DisposalBatchRequest{Scope: "scope", Versions: []model.VersionDisposalRequest{{Scope: "private", Bindings: []model.VersionRetentionBinding{{Scope: "private"}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{}
	entries, _, err := s.prepareAuditEntries(context.Background(), model.Caller{}, model.AuditExportRequest{Scope: "scope"}, []model.AuditEntry{{Kind: "retention", ID: "batch", Payload: raw}})
	if err != nil || len(entries) != 0 {
		t.Fatal("cross-scope batch leaked", entries, err)
	}
}

func TestDisposalBatchSnapshotContextNormalization(t *testing.T) {
	base := model.DisposalBatchRequest{Command: model.Command{Key: "batch", Purpose: "retention"}, Scope: "a", Reason: "expired",
		Snapshots: []model.SnapshotDisposalRequest{{SnapshotID: "snapshot", Bindings: []model.SnapshotRetentionBinding{{Scope: "a", PolicyID: "snapshots", Jurisdiction: "SG"}}}},
		Contexts:  []model.ContextDisposalRequest{{ContextID: "context", Bindings: []model.ContextRetentionBinding{{Scope: "z", PolicyID: "contexts", Jurisdiction: "SG"}, {Scope: "a", PolicyID: "contexts", Jurisdiction: "SG"}}}}}
	normalized, err := normalizeDisposalBatch(base)
	if err != nil || normalized.SelectionCount() != 2 || normalized.Snapshots[0].Command != base.Command || normalized.Contexts[0].Reason != base.Reason || normalized.Contexts[0].Bindings[0].Scope != "a" {
		t.Fatal(normalized, err)
	}
	if base.Contexts[0].Bindings[0].Scope != "z" || base.Snapshots[0].Command != (model.Command{}) {
		t.Fatal("normalization changed caller request")
	}
	for _, change := range []func(*model.DisposalBatchRequest){
		func(r *model.DisposalBatchRequest) { r.Snapshots = append(r.Snapshots, r.Snapshots[0]) },
		func(r *model.DisposalBatchRequest) { r.Contexts = append(r.Contexts, r.Contexts[0]) },
		func(r *model.DisposalBatchRequest) { r.Contexts[0].Command = model.Command{Key: "other"} },
		func(r *model.DisposalBatchRequest) { r.Snapshots[0].Reason = "other" },
		func(r *model.DisposalBatchRequest) { r.Contexts[0].Bindings = nil },
		func(r *model.DisposalBatchRequest) {
			r.Snapshots[0].Bindings = []model.SnapshotRetentionBinding{{Scope: "a", PolicyID: "", Jurisdiction: "SG"}}
		},
		func(r *model.DisposalBatchRequest) {
			for i := 0; i < 31; i++ {
				r.Versions = append(r.Versions, model.VersionDisposalRequest{})
			}
		},
	} {
		r := base
		r.Snapshots = append([]model.SnapshotDisposalRequest(nil), base.Snapshots...)
		r.Contexts = append([]model.ContextDisposalRequest(nil), base.Contexts...)
		change(&r)
		if _, err := normalizeDisposalBatch(r); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("invalid artifact selection accepted", r, err)
		}
	}
	scopes := base.Scopes()
	if len(scopes) != 2 || scopes[0] != "a" || scopes[1] != "z" {
		t.Fatal("incomplete disclosure scopes", scopes)
	}
}

func TestDisposalBatchCalculationNormalization(t *testing.T) {
	base := model.DisposalBatchRequest{Command: model.Command{Key: "batch", Purpose: "retention"}, Scope: "a", Reason: "expired", Calculations: []model.CalculationDisposalRequest{{CalculationID: "calculation", Bindings: []model.CalculationRetentionBinding{{Scope: "z", PolicyID: "policy", Jurisdiction: "SG"}, {Scope: "a", PolicyID: "policy", Jurisdiction: "SG"}}}}}
	normalized, err := normalizeDisposalBatch(base)
	if err != nil || normalized.SelectionCount() != 1 || normalized.Calculations[0].Command != base.Command || normalized.Calculations[0].Bindings[0].Scope != "a" || base.Calculations[0].Bindings[0].Scope != "z" {
		t.Fatal(normalized, err)
	}
	for _, change := range []func(*model.DisposalBatchRequest){
		func(r *model.DisposalBatchRequest) { r.Calculations = append(r.Calculations, r.Calculations[0]) },
		func(r *model.DisposalBatchRequest) { r.Calculations[0].Bindings = nil },
		func(r *model.DisposalBatchRequest) { r.Calculations[0].Command = model.Command{Key: "other"} },
		func(r *model.DisposalBatchRequest) { r.Calculations[0].Reason = "other" },
	} {
		r := base
		r.Calculations = append([]model.CalculationDisposalRequest(nil), base.Calculations...)
		change(&r)
		if _, err := normalizeDisposalBatch(r); !errors.Is(err, model.ErrInvalid) {
			t.Fatal(r, err)
		}
	}
	scopes := base.Scopes()
	if len(scopes) != 2 || scopes[0] != "a" || scopes[1] != "z" {
		t.Fatal(scopes)
	}
}

func TestDisposalBatchDecisionNormalization(t *testing.T) {
	base := model.DisposalBatchRequest{Command: model.Command{Key: "batch", Purpose: "retention"}, Scope: "a", Reason: "expired", Decisions: []model.DecisionDisposalRequest{{DecisionID: "decision", Bindings: []model.DecisionRetentionBinding{{Scope: "z", PolicyID: "policy", Jurisdiction: "SG"}, {Scope: "a", PolicyID: "policy", Jurisdiction: "SG"}}}}}
	normalized, err := normalizeDisposalBatch(base)
	if err != nil || normalized.SelectionCount() != 1 || normalized.Decisions[0].Command != base.Command || normalized.Decisions[0].Bindings[0].Scope != "a" || base.Decisions[0].Bindings[0].Scope != "z" {
		t.Fatal(normalized, err)
	}
	for _, change := range []func(*model.DisposalBatchRequest){
		func(r *model.DisposalBatchRequest) { r.Decisions = append(r.Decisions, r.Decisions[0]) },
		func(r *model.DisposalBatchRequest) { r.Decisions[0].Bindings = nil },
		func(r *model.DisposalBatchRequest) { r.Decisions[0].Command = model.Command{Key: "other"} },
		func(r *model.DisposalBatchRequest) { r.Decisions[0].Reason = "other" },
	} {
		r := base
		r.Decisions = append([]model.DecisionDisposalRequest(nil), base.Decisions...)
		change(&r)
		if _, err := normalizeDisposalBatch(r); !errors.Is(err, model.ErrInvalid) {
			t.Fatal(r, err)
		}
	}
	scopes := base.Scopes()
	if len(scopes) != 2 || scopes[0] != "a" || scopes[1] != "z" {
		t.Fatal(scopes)
	}
}
