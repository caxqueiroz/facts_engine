package workers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
)

type memoryProjectionSource struct{ doc model.ProjectionDocument }

func (s memoryProjectionSource) ProjectionDocument(context.Context, model.Delivery, string) (*model.ProjectionDocument, error) {
	return &s.doc, nil
}

type memoryProjectionIndex struct{ writes int }

func (i *memoryProjectionIndex) Put(context.Context, model.ProjectionDocument) error {
	i.writes++
	return nil
}
func (*memoryProjectionIndex) Candidates(context.Context, string, string, string, string, int) ([]model.SearchCandidate, error) {
	return nil, nil
}

type memoryProjectionAuth struct{ deny string }

func (a memoryProjectionAuth) Check(_ context.Context, q model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	return model.AuthorizationDecision{Allowed: q.Scope+"/"+q.Permission != a.deny, ModelID: "model", Consistency: "HIGHER_CONSISTENCY", CheckedAt: time.Now()}, nil
}
func TestMemoryProjectionRequiresDependencyPermissions(t *testing.T) {
	caller := model.Caller{Tenant: "tenant", Principal: "worker"}
	delivery := model.Delivery{Key: model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.ProjectionConsumer("g1")}, Event: model.Event{Type: "accept_memory", Scope: "own"}}
	source := memoryProjectionSource{model.ProjectionDocument{Memory: &model.MemoryProjection{Key: model.MemoryKey{Scope: "own", ID: "summary"}, Dependencies: []model.MemoryDependency{{Scope: "dependency"}}}}}
	for _, deny := range []string{"own/can_read_evidence", "dependency/can_read", "dependency/can_read_history", "dependency/can_read_evidence", ""} {
		t.Run(deny, func(t *testing.T) {
			index := &memoryProjectionIndex{}
			worker, err := workers.NewProjection(source, index, memoryProjectionAuth{deny}, caller, "g1")
			if err != nil {
				t.Fatal(err)
			}
			err = worker.Handle(context.Background(), delivery)
			if deny != "" {
				if !errors.Is(err, model.ErrDenied) || index.writes != 0 {
					t.Fatal("unauthorized memory indexed", err, index.writes)
				}
			} else if err != nil || index.writes != 1 {
				t.Fatal(err, index.writes)
			}
		})
	}
}
