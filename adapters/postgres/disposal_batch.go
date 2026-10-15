package postgres

import (
	"context"
	"sort"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) DisposeBatch(ctx context.Context, r model.DisposalBatchRequest) (model.DisposalBatchResult, error) {
	if r.SelectionCount() < 1 || r.SelectionCount() > 32 {
		return model.DisposalBatchResult{}, model.ErrInvalid
	}
	scopes := map[string]bool{}
	seen := map[string]bool{}
	facts, memories := []string{}, []string{}
	for _, v := range r.Versions {
		key := v.Kind + "/" + v.VersionID
		if !model.ValidID(v.VersionID) || !model.ValidID(v.RecordID) || !model.ValidID(v.Scope) || (v.Kind != "fact" && v.Kind != "memory") || seen[key] || v.Command != r.Command || v.Reason != r.Reason {
			return model.DisposalBatchResult{}, model.ErrInvalid
		}
		seen[key] = true
		if v.Kind == "fact" {
			facts = append(facts, v.VersionID)
		} else {
			memories = append(memories, v.VersionID)
		}
		for _, b := range v.Bindings {
			scopes[b.Scope] = true
		}
	}
	for _, e := range r.Extractions {
		key := "extraction/" + e.JobID
		if !model.ValidID(e.JobID) || seen[key] || e.Command != r.Command || e.Reason != r.Reason {
			return model.DisposalBatchResult{}, model.ErrInvalid
		}
		seen[key] = true
		for _, b := range e.Bindings {
			scopes[b.Scope] = true
		}
	}
	for _, item := range r.Conversations {
		key := "conversation/" + item.ConversationID
		if !model.ValidID(item.ConversationID) || !model.ValidID(item.Scope) || !model.ValidID(item.PolicyID) || !model.ValidID(item.Jurisdiction) || item.Purpose == "" || seen[key] || item.Command != r.Command || item.Reason != r.Reason {
			return model.DisposalBatchResult{}, model.ErrInvalid
		}
		seen[key] = true
		scopes[item.Scope] = true
	}
	for _, item := range r.Attestations {
		key := "attestation/" + item.Kind + "/" + item.AttestationID
		if (item.Kind != "fact" && item.Kind != "memory") || !model.ValidID(item.Scope) || !model.ValidID(item.RecordID) || !model.ValidID(item.VersionID) || !model.ValidID(item.AttestationID) || !model.ValidID(item.PolicyID) || !model.ValidID(item.Jurisdiction) || item.Purpose == "" || seen[key] || item.Command != r.Command || item.Reason != r.Reason {
			return model.DisposalBatchResult{}, model.ErrInvalid
		}
		seen[key] = true
		scopes[item.Scope] = true
	}
	for _, item := range r.Snapshots {
		key := "snapshot/" + item.SnapshotID
		if !model.ValidID(item.SnapshotID) || seen[key] || item.Command != r.Command || item.Reason != r.Reason {
			return model.DisposalBatchResult{}, model.ErrInvalid
		}
		seen[key] = true
		for _, b := range item.Bindings {
			scopes[b.Scope] = true
		}
	}
	for _, item := range r.Contexts {
		key := "context/" + item.ContextID
		if !model.ValidID(item.ContextID) || seen[key] || item.Command != r.Command || item.Reason != r.Reason {
			return model.DisposalBatchResult{}, model.ErrInvalid
		}
		seen[key] = true
		for _, b := range item.Bindings {
			scopes[b.Scope] = true
		}
	}
	for _, item := range r.Calculations {
		key := "calculation/" + item.CalculationID
		if !model.ValidID(item.CalculationID) || seen[key] || item.Command != r.Command || item.Reason != r.Reason {
			return model.DisposalBatchResult{}, model.ErrInvalid
		}
		seen[key] = true
		for _, b := range item.Bindings {
			scopes[b.Scope] = true
		}
	}
	for _, item := range r.Decisions {
		key := "decision/" + item.DecisionID
		if !model.ValidID(item.DecisionID) || seen[key] || item.Command != r.Command || item.Reason != r.Reason {
			return model.DisposalBatchResult{}, model.ErrInvalid
		}
		seen[key] = true
		for _, b := range item.Bindings {
			scopes[b.Scope] = true
		}
	}
	if len(scopes) > 256 || !scopes[r.Scope] {
		return model.DisposalBatchResult{}, model.ErrDenied
	}
	ordered := make([]string, 0, len(scopes))
	for s := range scopes {
		if !model.ValidID(s) {
			return model.DisposalBatchResult{}, model.ErrInvalid
		}
		ordered = append(ordered, s)
	}
	sort.Strings(ordered)
	for _, s := range ordered {
		if err := t.lockRetentionScope(ctx, s); err != nil {
			return model.DisposalBatchResult{}, err
		}
	}
	if err := ledgerdb.New(t.tx).LockProjectionRetentionGuard(ctx, t.tenant); err != nil {
		return model.DisposalBatchResult{}, err
	}
	itemsDecisions := append([]model.DecisionDisposalRequest(nil), r.Decisions...)
	sort.Slice(itemsDecisions, func(i, j int) bool { return itemsDecisions[i].DecisionID < itemsDecisions[j].DecisionID })
	for _, item := range itemsDecisions {
		if err := t.DisposeDecision(ctx, item); err != nil {
			return model.DisposalBatchResult{}, err
		}
	}
	itemsCalculations := append([]model.CalculationDisposalRequest(nil), r.Calculations...)
	sort.Slice(itemsCalculations, func(i, j int) bool { return itemsCalculations[i].CalculationID < itemsCalculations[j].CalculationID })
	for _, item := range itemsCalculations {
		if err := t.DisposeCalculation(ctx, item); err != nil {
			return model.DisposalBatchResult{}, err
		}
	}
	itemsSnapshots := append([]model.SnapshotDisposalRequest(nil), r.Snapshots...)
	sort.Slice(itemsSnapshots, func(i, j int) bool { return itemsSnapshots[i].SnapshotID < itemsSnapshots[j].SnapshotID })
	for _, item := range itemsSnapshots {
		if err := t.DisposeSnapshot(ctx, item); err != nil {
			return model.DisposalBatchResult{}, err
		}
	}
	itemsContexts := append([]model.ContextDisposalRequest(nil), r.Contexts...)
	sort.Slice(itemsContexts, func(i, j int) bool { return itemsContexts[i].ContextID < itemsContexts[j].ContextID })
	for _, item := range itemsContexts {
		if err := t.DisposeContext(ctx, item); err != nil {
			return model.DisposalBatchResult{}, err
		}
	}
	jobs := append([]model.ExtractionDisposalRequest(nil), r.Extractions...)
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].JobID < jobs[j].JobID })
	for _, e := range jobs {
		if err := t.disposeExtraction(ctx, e, facts, memories); err != nil {
			return model.DisposalBatchResult{}, err
		}
	}
	attestations := append([]model.AttestationDisposalRequest(nil), r.Attestations...)
	attestationRevisions := make([]int64, len(r.Attestations))
	sort.Slice(attestations, func(i, j int) bool {
		if attestations[i].Kind != attestations[j].Kind {
			return attestations[i].Kind < attestations[j].Kind
		}
		return attestations[i].AttestationID < attestations[j].AttestationID
	})
	for _, item := range attestations {
		revision, err := t.DisposeAttestation(ctx, item)
		if err != nil {
			return model.DisposalBatchResult{}, err
		}
		for i, selected := range r.Attestations {
			if selected.Kind == item.Kind && selected.AttestationID == item.AttestationID {
				attestationRevisions[i] = revision
				break
			}
		}
	}
	conversations := append([]model.ConversationDisposalRequest(nil), r.Conversations...)
	sort.Slice(conversations, func(i, j int) bool { return conversations[i].ConversationID < conversations[j].ConversationID })
	for _, item := range conversations {
		if err := t.DisposeConversation(ctx, item); err != nil {
			return model.DisposalBatchResult{}, err
		}
	}
	revisions := make([]int64, len(r.Versions))
	// Memory dependencies reference facts. Remove their selected bodies first;
	// any unselected memory/extraction copy still blocks the fact below.
	for _, kind := range []string{"memory", "fact"} {
		for i, v := range r.Versions {
			if v.Kind != kind {
				continue
			}
			revision, err := t.DisposeVersion(ctx, v)
			if err != nil {
				return model.DisposalBatchResult{}, err
			}
			revisions[i] = revision
		}
	}
	return model.DisposalBatchResult{VersionRevisions: revisions, AttestationRevisions: attestationRevisions}, nil
}

var _ model.DisposalBatchTx = (*transaction)(nil)
