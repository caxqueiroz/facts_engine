package workers

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

type fixedWorkerAuth struct{ decision model.AuthorizationDecision }

func (a fixedWorkerAuth) Check(context.Context, model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	return a.decision, nil
}

// Embedded nil interfaces panic if an invalid permission reaches a dependency.
type forbiddenProjectionSource struct{ model.ProjectionSource }
type forbiddenWorkerIndex struct{ model.RetrievalIndex }
type forbiddenArchiveStore struct{ model.ArchiveStore }
type forbiddenArchiveSigner struct{ model.AuditSigner }
type authorizationArchiveSource struct{}

func (authorizationArchiveSource) ArchivePayload(_ context.Context, d model.Delivery) (model.ArchivePayload, error) {
	return model.ArchivePayload{Event: d.Event, Scopes: []string{"scope"}}, nil
}

func TestWorkersRejectInvalidAuthorizationBeforePublication(t *testing.T) {
	for _, name := range []string{"denied", "stale", "future", "missing_model", "cached", "missing_time"} {
		t.Run(name, func(t *testing.T) {
			decision := model.AuthorizationDecision{Allowed: true, ModelID: "model", Consistency: "HIGHER_CONSISTENCY", CheckedAt: time.Now()}
			want := model.ErrUnavailable
			switch name {
			case "denied":
				decision.Allowed = false
				want = model.ErrDenied
			case "stale":
				decision.CheckedAt = time.Now().Add(-time.Minute)
			case "future":
				decision.CheckedAt = time.Now().Add(time.Minute)
			case "missing_model":
				decision.ModelID = ""
			case "cached":
				decision.Consistency = "MINIMIZE_LATENCY"
			case "missing_time":
				decision.CheckedAt = time.Time{}
			}
			caller := model.Caller{Tenant: "tenant", Principal: "worker", Service: true}
			auth := fixedWorkerAuth{decision}
			projection, err := NewProjection(forbiddenProjectionSource{}, forbiddenWorkerIndex{}, auth, caller, "generation")
			if err != nil {
				t.Fatal(err)
			}
			delivery := model.Delivery{Key: model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.ProjectionConsumer("generation")}, Event: model.Event{ID: "event", Type: "accept_fact", Scope: "scope", Caller: caller, LedgerRevision: 1}}
			if err := projection.Handle(context.Background(), delivery); !errors.Is(err, want) {
				t.Fatalf("projection: %v", err)
			}
			archive, err := NewArchive(authorizationArchiveSource{}, forbiddenArchiveStore{}, forbiddenArchiveSigner{}, map[string]ed25519.PublicKey{"key": make([]byte, ed25519.PublicKeySize)}, auth, caller, "stream")
			if err != nil {
				t.Fatal(err)
			}
			runner := &auditRunner{}
			audit, err := NewAuditExport(runner, auth, caller, "stream")
			if err != nil {
				t.Fatal(err)
			}
			auditDelivery := delivery
			auditDelivery.Key.Consumer = model.AuditExportConsumer("stream")
			auditDelivery.Fence = 1
			auditDelivery.Event.Type = "start_audit_export"
			auditDelivery.Event.EntityID = "export"
			if err := audit.Handle(context.Background(), auditDelivery); !errors.Is(err, want) || runner.calls != 0 {
				t.Fatalf("audit: %v, calls %d", err, runner.calls)
			}
			delivery.Key.Consumer = model.ArchiveConsumer("stream")
			if err := archive.Handle(context.Background(), delivery); !errors.Is(err, want) {
				t.Fatalf("archive: %v", err)
			}
		})
	}
}
