package services

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

type interruptedRegistry struct {
	read func() (model.ActiveProjection, error)
}

func (r interruptedRegistry) ActiveProjection(context.Context, string) (model.ActiveProjection, error) {
	return r.read()
}

type interruptedProgress struct {
	model.Outbox
	read func() (model.ConsumerStatus, error)
}

func (p interruptedProgress) ConsumerStatus(context.Context, model.ConsumerKey) (model.ConsumerStatus, error) {
	return p.read()
}

type interruptedIndex struct {
	model.RetrievalIndex
	read func() ([]model.SearchCandidate, error)
}

func (i interruptedIndex) Candidates(context.Context, string, string, string, string, int) ([]model.SearchCandidate, error) {
	return i.read()
}

func TestRetrievalDependencyInterruption(t *testing.T) {
	caller := model.Caller{Tenant: "tenant", Principal: "reader"}
	query := model.RecallRequest{Scopes: []string{"scope1", "scope2"}, Query: "query", Purpose: "review", ValidAt: time.Now(), Limit: 2}
	for _, stage := range []string{"registry", "progress", "first search", "second search"} {
		for _, want := range []error{context.Canceled, context.DeadlineExceeded, model.ErrUnavailable} {
			t.Run(stage+"/"+want.Error(), func(t *testing.T) {
				failure := fmt.Errorf("PRIVATE dependency: %w", want)
				calls := 0
				s := &Service{auth: waitAuth{check: func() bool { return true }}, retrieval: model.RetrievalOptions{
					Registry: interruptedRegistry{read: func() (model.ActiveProjection, error) {
						if stage == "registry" {
							return model.ActiveProjection{}, failure
						}
						return model.ActiveProjection{Generation: "g1"}, nil
					}},
					Progress: interruptedProgress{read: func() (model.ConsumerStatus, error) {
						if stage == "progress" {
							return model.ConsumerStatus{}, failure
						}
						return model.ConsumerStatus{AcknowledgedRevision: 10}, nil
					}},
					Index: interruptedIndex{read: func() ([]model.SearchCandidate, error) {
						calls++
						if stage == "first search" || (stage == "second search" && calls == 2) {
							return nil, failure
						}
						return []model.SearchCandidate{{RecordID: "record", VersionID: "version"}}, nil
					}},
				}}
				_, _, _, candidates, err := s.searchRecall(context.Background(), caller, query)
				if err != want || len(candidates) != 0 {
					t.Fatal("dependency failure leaked detail or partial candidates", candidates, err)
				}
			})
		}
	}
}

func TestProjectionWaitWrappedInterruption(t *testing.T) {
	for _, stage := range []string{"registry", "progress", "final registry"} {
		for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
			t.Run(stage+"/"+want.Error(), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				checks := 0
				failure := fmt.Errorf("PRIVATE: %w", want)
				s := &Service{auth: waitAuth{check: func() bool { return true }}, retrieval: model.RetrievalOptions{Index: waitIndex{}, Registry: interruptedRegistry{read: func() (model.ActiveProjection, error) {
					checks++
					if stage == "registry" || (stage == "final registry" && checks == 2) {
						return model.ActiveProjection{}, failure
					}
					return model.ActiveProjection{Generation: "g1"}, nil
				}}, Progress: interruptedProgress{read: func() (model.ConsumerStatus, error) {
					if stage == "progress" {
						return model.ConsumerStatus{}, failure
					}
					return model.ConsumerStatus{AcknowledgedRevision: 10}, nil
				}}}}
				receipt, err := s.WaitForProjection(ctx, model.Caller{Tenant: "tenant", Principal: "reader"}, model.ProjectionWaitRequest{Scopes: []string{"scope"}, Generation: "g1", Revision: 10, Purpose: "review"})
				if err != want || receipt != (model.ProjectionReceipt{}) {
					t.Fatal("interruption became unavailable or successful receipt", receipt, err)
				}
			})
		}
	}
}

func TestDependencyLateCancellationAndSanitization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if unavailableDependency(ctx, nil) != context.Canceled {
		t.Fatal("late cancellation ignored")
	}
	if unavailableDependency(context.Background(), errors.New("PRIVATE")) != model.ErrUnavailable {
		t.Fatal("dependency detail exposed")
	}
	for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
		s := &Service{contextArchive: &model.ContextArchiveOptions{Stream: "archive", Progress: contextProgressFunc(func(context.Context, model.ConsumerKey) (model.OutboxProgress, error) {
			return model.OutboxProgress{}, fmt.Errorf("PRIVATE: %w", want)
		})}}
		receipt, err := s.checkContextArchive(context.Background(), "tenant", 0)
		if err != want || receipt != nil {
			t.Fatal("archive progress interruption lost", receipt, err)
		}
	}
}
