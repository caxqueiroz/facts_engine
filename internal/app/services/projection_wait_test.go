package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

type waitProgress struct {
	model.Outbox
	read func() model.ConsumerStatus
}

func (p waitProgress) ConsumerStatus(context.Context, model.ConsumerKey) (model.ConsumerStatus, error) {
	return p.read(), nil
}

type waitAuth struct{ check func() bool }

func (a waitAuth) Check(context.Context, model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	return model.AuthorizationDecision{Allowed: a.check(), ModelID: "test", Consistency: "HIGHER_CONSISTENCY", CheckedAt: time.Now()}, nil
}

type waitIndex struct{ model.RetrievalIndex }
type waitRegistry struct{ read func() model.ActiveProjection }

func (r waitRegistry) ActiveProjection(context.Context, string) (model.ActiveProjection, error) {
	return r.read(), nil
}

func TestProjectionWait(t *testing.T) {
	c := model.Caller{Tenant: "tenant", Principal: "reader"}
	q := model.ProjectionWaitRequest{Scopes: []string{"scope"}, Generation: "g1", Revision: 10, Purpose: "review"}
	for _, name := range []string{"ready", "advance", "deadline", "failed", "revoked", "cutover", "no-deadline"} {
		t.Run(name, func(t *testing.T) {
			checks, polls, registryChecks := 0, 0, 0
			s := &Service{auth: waitAuth{check: func() bool { checks++; return name != "revoked" || checks == 1 }}, retrieval: model.RetrievalOptions{Generation: "g1", Index: waitIndex{}, Progress: waitProgress{read: func() model.ConsumerStatus {
				polls++
				if name == "deadline" || name == "failed" || (name == "advance" && polls == 1) {
					state := "pending"
					if name == "failed" {
						state = "failed"
					}
					return model.ConsumerStatus{State: state}
				}
				return model.ConsumerStatus{AcknowledgedRevision: 10}
			}}}}
			if name == "cutover" {
				s.retrieval.Registry = waitRegistry{read: func() model.ActiveProjection {
					registryChecks++
					return model.ActiveProjection{Generation: "g1", Epoch: int64(registryChecks)}
				}}
			}
			timeout := time.Second
			if name == "deadline" {
				timeout = 20 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if name == "no-deadline" {
				ctx = context.Background()
			}
			receipt, err := s.WaitForProjection(ctx, c, q)
			expected := map[string]error{"deadline": context.DeadlineExceeded, "failed": model.ErrUnavailable, "revoked": model.ErrDenied, "cutover": model.ErrUnavailable, "no-deadline": model.ErrInvalid}[name]
			if !errors.Is(err, expected) {
				t.Fatalf("receipt=%v error=%v want=%v", receipt, err, expected)
			}
			if expected == nil && (receipt.IndexedRevision != 10 || checks != 2) {
				t.Fatal(receipt, checks)
			}
			if expected != nil && receipt != (model.ProjectionReceipt{}) {
				t.Fatal("partial receipt on error", receipt)
			}
		})
	}
}
