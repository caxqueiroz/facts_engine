package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func (t *extractionDisposalTestTx) DisposeBatch(ctx context.Context, r model.DisposalBatchRequest) ([]int64, error) {
	err := t.DisposeExtraction(ctx, model.ExtractionDisposalRequest{})
	revisions := make([]int64, len(r.Versions))
	for i := range revisions {
		revisions[i] = int64(i + 2)
	}
	return revisions, err
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
