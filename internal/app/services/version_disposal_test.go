package services

import (
	"context"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func (t *extractionDisposalTestTx) DisposeVersion(ctx context.Context, _ model.VersionDisposalRequest) (int64, error) {
	return 5, t.DisposeExtraction(ctx, model.ExtractionDisposalRequest{})
}
func TestVersionDisposalKeepsOwningScopeAndReauthorizes(t *testing.T) {
	for _, phase := range []string{"success", "initial", "after-disposal", "before-commit"} {
		t.Run(phase, func(t *testing.T) {
			s, l, a, c, _ := extractionDisposalFixture()
			r := model.VersionDisposalRequest{Command: model.Command{Key: "dispose", Purpose: "retention"}, Kind: "memory", Scope: "z", RecordID: "record", VersionID: "version", Reason: "expired", Bindings: []model.VersionRetentionBinding{{Scope: "z", PolicyID: "policy", Jurisdiction: "XX"}, {Scope: "a", PolicyID: "policy", Jurisdiction: "XX"}}}
			revoke := func() { a.denied = "a" }
			switch phase {
			case "initial":
				revoke()
			case "after-disposal":
				l.tx.afterDispose = revoke
			case "before-commit":
				l.tx.afterAppend = revoke
			}
			first, err := s.DisposeVersion(context.Background(), c, r)
			if phase != "success" {
				if !errors.Is(err, model.ErrDenied) || l.tx.disposed != 0 || l.tx.receipt != nil {
					t.Fatal(err, l.tx)
				}
				return
			}
			if err != nil || first.StreamRevision != 5 || first.RecordKind != "memory" || first.RecordID != r.RecordID || first.VersionID != r.VersionID || l.tx.event.Scope != "z" || l.tx.event.Kind != "dispose_memory_payload" || len(l.tx.event.AdditionalAuthorization) != 2 {
				t.Fatal(first, err, l.tx.event)
			}
			r.Bindings[0], r.Bindings[1] = r.Bindings[1], r.Bindings[0]
			replay, err := s.DisposeVersion(context.Background(), c, r)
			if err != nil || replay != first || l.tx.disposed != 1 {
				t.Fatal(replay, err)
			}
			r.Bindings[0].PolicyID = "changed"
			if _, err = s.DisposeVersion(context.Background(), c, r); !errors.Is(err, model.ErrConflict) {
				t.Fatal(err)
			}
		})
	}
}
