package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func (t *extractionDisposalTestTx) PrepareProjectionCleanup(ctx context.Context, r model.ProjectionCleanupRequest) (model.ProjectionCleanupPlan, error) {
	err := t.DisposeExtraction(ctx, model.ExtractionDisposalRequest{})
	return model.ProjectionCleanupPlan{Target: model.ProjectionDisposal{Tenant: "tenant", Kind: r.Kind, VersionID: r.VersionID, Generation: r.Generation}}, err
}

type cleanupDestination struct {
	calls int
	err   error
	after func()
}

func (d *cleanupDestination) DisposeProjection(context.Context, model.ProjectionDisposal) error {
	d.calls++
	if d.after != nil {
		d.after()
	}
	return d.err
}
func TestProjectionCleanupAuthorizationAndReplay(t *testing.T) {
	for _, phase := range []string{"success", "before-call", "after-call", "before-commit", "unavailable"} {
		t.Run(phase, func(t *testing.T) {
			s, l, a, c, _ := extractionDisposalFixture()
			destination := &cleanupDestination{}
			s.projectionCleanup.Destination = destination
			r := model.ProjectionCleanupRequest{Command: model.Command{Key: "cleanup", Purpose: "retention"}, Kind: "memory", Scope: "z", RecordID: "record", VersionID: "version", DisposalEventID: "disposal", Generation: "index", Reason: "expired", Bindings: []model.VersionRetentionBinding{{Scope: "z", PolicyID: "policy", Jurisdiction: "SG"}, {Scope: "a", PolicyID: "policy", Jurisdiction: "SG"}}}
			revoke := func() { a.denied = "a" }
			switch phase {
			case "before-call":
				l.tx.afterDispose = revoke
			case "after-call":
				destination.after = revoke
			case "before-commit":
				l.tx.afterAppend = revoke
			case "unavailable":
				destination.err = errors.New("private endpoint details")
			}
			first, err := s.CleanupProjection(context.Background(), c, r)
			if phase != "success" {
				want := model.ErrDenied
				if phase == "unavailable" {
					want = model.ErrUnavailable
				}
				if !errors.Is(err, want) || l.tx.receipt != nil {
					t.Fatal(first, err, l.tx.receipt)
				}
				if phase == "before-call" && destination.calls != 0 {
					t.Fatal("called after revocation")
				}
				return
			}
			if err != nil || destination.calls != 1 || l.tx.event.Scope != "z" || l.tx.event.ProjectionCleanup == nil || len(l.tx.event.AdditionalAuthorization) != 2 {
				t.Fatal(first, err, l.tx.event, destination.calls)
			}
			r.Bindings[0], r.Bindings[1] = r.Bindings[1], r.Bindings[0]
			replay, err := s.CleanupProjection(context.Background(), c, r)
			if err != nil || first != replay || destination.calls != 1 {
				t.Fatal(replay, err, destination.calls)
			}
			revoke()
			if _, err = s.CleanupProjection(context.Background(), c, r); !errors.Is(err, model.ErrDenied) {
				t.Fatal("revoked receipt disclosed", err)
			}
		})
	}
}

func TestProjectionCleanupAuditWithholdsDependencyScopes(t *testing.T) {
	for _, crossScope := range []bool{false, true} {
		bindings := []model.VersionRetentionBinding{{Scope: "scope"}}
		if crossScope {
			bindings = append(bindings, model.VersionRetentionBinding{Scope: "private"})
		}
		raw, err := json.Marshal(model.RetentionEvent{Kind: "cleanup_projection", ProjectionCleanup: &model.ProjectionCleanupRequest{Bindings: bindings}})
		if err != nil {
			t.Fatal(err)
		}
		s := &Service{}
		out, _, err := s.prepareAuditEntries(context.Background(), model.Caller{}, model.AuditExportRequest{Scope: "scope"}, []model.AuditEntry{{Kind: "retention", ID: "event", Payload: raw}})
		if err != nil || (len(out) == 1) == crossScope {
			t.Fatal(crossScope, out, err)
		}
	}
}
