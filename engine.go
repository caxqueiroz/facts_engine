// Package factsengine exposes the embeddable, policy-governed fact engine.
package factsengine

import (
	"context"
	"github.com/caxqueiroz/facts_engine/internal/app/services"
	"github.com/caxqueiroz/facts_engine/model"
)

// Engine uses injected dependencies. New does not migrate, start goroutines, or own connections.
type Engine struct{ service *services.Service }

func New(ledger Ledger, auth Authorizer, evidence EvidenceStore) (*Engine, error) {
	s, err := services.New(ledger, auth, evidence)
	if err != nil {
		return nil, err
	}
	return &Engine{s}, nil
}
func (e *Engine) RegisterAcceptancePolicy(ctx context.Context, c Caller, r PolicyRequest) (CommitResult, error) {
	return e.service.RegisterAcceptancePolicy(ctx, c, r)
}
func (e *Engine) RegisterPredicateVersion(ctx context.Context, c Caller, r PredicateRequest) (CommitResult, error) {
	return e.service.RegisterPredicateVersion(ctx, c, r)
}
func (e *Engine) RegisterEvidence(ctx context.Context, c Caller, r EvidenceRequest) (CommitResult, error) {
	return e.service.RegisterEvidence(ctx, c, r)
}
func (e *Engine) ProposeFact(ctx context.Context, c Caller, r FactProposal) (CommitResult, error) {
	return e.service.ProposeFact(ctx, c, r)
}
func (e *Engine) RecordAttestation(ctx context.Context, c Caller, r AttestationRequest) (CommitResult, error) {
	return e.service.RecordAttestation(ctx, c, r)
}
func (e *Engine) WithdrawAttestation(ctx context.Context, c Caller, r Withdrawal) (CommitResult, error) {
	return e.service.WithdrawAttestation(ctx, c, r)
}
func (e *Engine) ApproveFact(ctx context.Context, c Caller, r Approval) (CommitResult, error) {
	return e.service.ApproveFact(ctx, c, r)
}
func (e *Engine) AcceptFact(ctx context.Context, c Caller, r Approval) (CommitResult, error) {
	return e.service.AcceptFact(ctx, c, r)
}
func (e *Engine) GetFact(ctx context.Context, c Caller, r FactQuery) (FactResult, error) {
	return e.service.GetFact(ctx, c, r)
}

type Caller = model.Caller
type Command = model.Command
type FactProposal = model.FactProposal
type Approval = model.Approval
type FactQuery = model.FactQuery
type FactResult = model.FactResult
type CommitResult = model.CommitResult
type PredicateRequest = model.PredicateRequest
type PolicyRequest = model.PolicyRequest
type EvidenceRequest = model.EvidenceRequest
type AttestationRequest = model.AttestationRequest
type Withdrawal = model.Withdrawal
type Ledger = model.Ledger
type Authorizer = model.Authorizer
type EvidenceStore = model.EvidenceStore
type Value = model.Value
type FactKey = model.FactKey
type Interval = model.Interval

var (
	ErrInvalid      = model.ErrInvalid
	ErrDenied       = model.ErrDenied
	ErrNotFound     = model.ErrNotFound
	ErrConflict     = model.ErrConflict
	ErrRevision     = model.ErrRevision
	ErrPrecondition = model.ErrPrecondition
	ErrUnavailable  = model.ErrUnavailable
)
