package model

import (
	"context"
	"sort"
)

// DisposalBatchRequest explicitly selects related version, extraction,
// snapshot, context and calculation payloads. Child commands/reasons may be empty or equal to the parent; the
// service normalizes them to the parent before authorization and replay checks.
// Other retained copies remain blockers. Index removal remains asynchronous.
type DisposalBatchRequest struct {
	Command
	Scope, Reason string
	Calculations  []CalculationDisposalRequest `json:",omitempty"`
	Snapshots     []SnapshotDisposalRequest    `json:",omitempty"`
	Contexts      []ContextDisposalRequest     `json:",omitempty"`
	Versions      []VersionDisposalRequest
	Extractions   []ExtractionDisposalRequest
}

// DisposalBatchTx must remove the complete authorized selection atomically,
// including all individual policy/age/hold/reference checks. Returned revisions
// correspond to Versions in input order. Any error must abort the transaction.
type DisposalBatchTx interface {
	DisposeBatch(context.Context, DisposalBatchRequest) ([]int64, error)
}

// Scopes includes every scope carried by a batch for audit/archive disclosure.
func (r DisposalBatchRequest) Scopes() []string {
	scopes := map[string]bool{r.Scope: true}
	for _, v := range r.Versions {
		scopes[v.Scope] = true
		for _, b := range v.Bindings {
			scopes[b.Scope] = true
		}
	}
	for _, e := range r.Extractions {
		for _, b := range e.Bindings {
			scopes[b.Scope] = true
		}
	}
	for _, item := range r.Snapshots {
		for _, b := range item.Bindings {
			scopes[b.Scope] = true
		}
	}
	for _, item := range r.Contexts {
		for _, b := range item.Bindings {
			scopes[b.Scope] = true
		}
	}
	for _, item := range r.Calculations {
		for _, b := range item.Bindings {
			scopes[b.Scope] = true
		}
	}
	out := make([]string, 0, len(scopes))
	for s := range scopes {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// SelectionCount bounds the complete transaction across all artifact classes.
func (r DisposalBatchRequest) SelectionCount() int {
	return len(r.Versions) + len(r.Extractions) + len(r.Snapshots) + len(r.Contexts) + len(r.Calculations)
}
