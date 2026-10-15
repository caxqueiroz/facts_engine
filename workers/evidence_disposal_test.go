package workers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

type evidenceDisposalArchiveSource struct{ payload model.ArchivePayload }

func (s evidenceDisposalArchiveSource) ArchivePayload(context.Context, model.Delivery) (model.ArchivePayload, error) {
	return s.payload, nil
}

type evidenceVersionDeleteCall struct {
	tenant string
	object model.ObjectRef
}

type evidenceVersionDeleteStore struct {
	calls []evidenceVersionDeleteCall
	fail  string
}

func (s *evidenceVersionDeleteStore) DeleteEvidenceVersion(_ context.Context, tenant string, object model.ObjectRef) error {
	s.calls = append(s.calls, evidenceVersionDeleteCall{tenant, object})
	if object.Locator == s.fail {
		s.fail = ""
		return model.ErrUnavailable
	}
	return nil
}

func TestEvidenceDisposalHandlerRetriesExactObjectVersions(t *testing.T) {
	disposal := model.RetentionEvent{ID: "retention-event", Tenant: "tenant", Scope: "scope", Kind: "dispose_evidence", TargetID: "root", LedgerRevision: 9, Disposal: &model.EvidenceDisposalRequest{Scope: "scope", EvidenceID: "root", DerivativeEvidenceIDs: []string{"child"}}}
	retentionBytes, err := json.Marshal(disposal)
	if err != nil {
		t.Fatal(err)
	}
	root := model.Evidence{ID: "root", Scope: "scope", Object: model.ObjectRef{Locator: "root-object", Version: "root-v1", Digest: "root-digest", Size: 4}}
	child := model.Evidence{ID: "child", Scope: "scope", Object: model.ObjectRef{Locator: "child-object", Version: "child-v1", Digest: "child-digest", Size: 5}}
	rootBytes, _ := json.Marshal(root)
	childBytes, _ := json.Marshal(child)
	delivery := model.Delivery{Key: model.ConsumerKey{Tenant: "tenant", Consumer: model.EvidenceDisposalConsumer()}, Event: model.Event{ID: "outbox-event", EntityID: disposal.ID, Type: "dispose_evidence", Scope: "scope", LedgerRevision: 9, Caller: model.Caller{Tenant: "tenant"}}}
	source := evidenceDisposalArchiveSource{payload: model.ArchivePayload{Event: delivery.Event, Scopes: []string{"scope"}, Entries: []model.AuditEntry{{Kind: "retention", ID: disposal.ID, Payload: retentionBytes}, {Kind: "evidence", ID: root.ID, Payload: rootBytes}, {Kind: "evidence", ID: child.ID, Payload: childBytes}}}}
	store := &evidenceVersionDeleteStore{fail: "child-object"}
	handler, err := NewEvidenceDisposalHandler(source, store, "tenant")
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), delivery); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("partial deletion failure not surfaced for outbox retry", err)
	}
	if err := handler.Handle(context.Background(), delivery); err != nil {
		t.Fatal("idempotent retry failed", err)
	}
	if len(store.calls) != 4 {
		t.Fatal("retry did not replay all exact versions", store.calls)
	}
	for i, want := range []string{"root-object", "child-object", "root-object", "child-object"} {
		if store.calls[i].tenant != "tenant" || store.calls[i].object.Locator != want {
			t.Fatal("wrong deletion target", i, store.calls[i])
		}
	}
}

func TestEvidenceDisposalHandlerIgnoresOtherEventsAndRejectsWrongConsumer(t *testing.T) {
	store := &evidenceVersionDeleteStore{}
	handler, err := NewEvidenceDisposalHandler(evidenceDisposalArchiveSource{}, store, "tenant")
	if err != nil {
		t.Fatal(err)
	}
	delivery := model.Delivery{Key: model.ConsumerKey{Tenant: "tenant", Consumer: model.EvidenceDisposalConsumer()}, Event: model.Event{Type: "propose_fact", LedgerRevision: 1}}
	if err := handler.Handle(context.Background(), delivery); err != nil || len(store.calls) != 0 {
		t.Fatal("unrelated event was not a no-op", err, store.calls)
	}
	delivery.Key.Consumer = "other"
	if err := handler.Handle(context.Background(), delivery); !errors.Is(err, model.ErrDenied) {
		t.Fatal("wrong consumer accepted", err)
	}
}
