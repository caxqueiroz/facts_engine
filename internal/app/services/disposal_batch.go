package services

import (
	"context"
	"sort"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func normalizeDisposalBatch(r model.DisposalBatchRequest) (model.DisposalBatchRequest, error) {
	if !model.ValidID(r.Scope) || r.Reason == "" || len(r.Reason) > 1024 || r.SelectionCount() < 1 || r.SelectionCount() > 32 {
		return r, model.ErrInvalid
	}
	r.Versions = append([]model.VersionDisposalRequest(nil), r.Versions...)
	r.Extractions = append([]model.ExtractionDisposalRequest(nil), r.Extractions...)
	r.Evidence = append([]model.EvidenceDisposalRequest(nil), r.Evidence...)
	r.Conversations = append([]model.ConversationDisposalRequest(nil), r.Conversations...)
	r.Attestations = append([]model.AttestationDisposalRequest(nil), r.Attestations...)
	selected := map[string]bool{}
	scopes := map[string]bool{}
	for i := range r.Evidence {
		item := &r.Evidence[i]
		if !model.ValidID(item.Scope) || !model.ValidID(item.EvidenceID) || !model.ValidID(item.PolicyID) || !model.ValidID(item.Jurisdiction) || r.Purpose == "" || len(r.Purpose) > 256 || (item.Command != (model.Command{}) && item.Command != r.Command) || (item.Reason != "" && item.Reason != r.Reason) || len(item.DerivativeEvidenceIDs) > 31 {
			return r, model.ErrInvalid
		}
		item.DerivativeEvidenceIDs = append([]string(nil), item.DerivativeEvidenceIDs...)
		key := "evidence/" + item.EvidenceID
		if selected[key] {
			return r, model.ErrInvalid
		}
		selected[key] = true
		item.Command, item.Reason = r.Command, r.Reason
		for _, id := range item.DerivativeEvidenceIDs {
			key = "evidence/" + id
			if !model.ValidID(id) || selected[key] {
				return r, model.ErrInvalid
			}
			selected[key] = true
		}
		sort.Strings(item.DerivativeEvidenceIDs)
		scopes[item.Scope] = true
	}
	sort.Slice(r.Evidence, func(i, j int) bool { return r.Evidence[i].EvidenceID < r.Evidence[j].EvidenceID })
	for i := range r.Versions {
		v := &r.Versions[i]
		key := v.Kind + "/" + v.VersionID
		if (v.Kind != "fact" && v.Kind != "memory") || !model.ValidID(v.Scope) || !model.ValidID(v.RecordID) || !model.ValidID(v.VersionID) || selected[key] || len(v.Bindings) < 1 || len(v.Bindings) > 256 || (v.Command != (model.Command{}) && v.Command != r.Command) || (v.Reason != "" && v.Reason != r.Reason) {
			return r, model.ErrInvalid
		}
		selected[key] = true
		v.Command = r.Command
		v.Reason = r.Reason
		v.Bindings = append([]model.VersionRetentionBinding(nil), v.Bindings...)
		sort.Slice(v.Bindings, func(a, b int) bool { return v.Bindings[a].Scope < v.Bindings[b].Scope })
		own := false
		for j, b := range v.Bindings {
			if !model.ValidID(b.Scope) || !model.ValidID(b.PolicyID) || !model.ValidID(b.Jurisdiction) || (j > 0 && v.Bindings[j-1].Scope == b.Scope) {
				return r, model.ErrInvalid
			}
			scopes[b.Scope] = true
			own = own || b.Scope == v.Scope
		}
		if !own {
			return r, model.ErrInvalid
		}
	}
	for i := range r.Extractions {
		e := &r.Extractions[i]
		key := "extraction/" + e.JobID
		if !model.ValidID(e.JobID) || selected[key] || len(e.Bindings) < 1 || len(e.Bindings) > 256 || (e.Command != (model.Command{}) && e.Command != r.Command) || (e.Reason != "" && e.Reason != r.Reason) {
			return r, model.ErrInvalid
		}
		selected[key] = true
		e.Command = r.Command
		e.Reason = r.Reason
		e.Bindings = append([]model.ExtractionRetentionBinding(nil), e.Bindings...)
		sort.Slice(e.Bindings, func(a, b int) bool { return e.Bindings[a].Scope < e.Bindings[b].Scope })
		for j, b := range e.Bindings {
			if !model.ValidID(b.Scope) || !model.ValidID(b.JobPolicyID) || !model.ValidID(b.RequestPolicyID) || !model.ValidID(b.ResponsePolicyID) || !model.ValidID(b.Jurisdiction) || (j > 0 && e.Bindings[j-1].Scope == b.Scope) {
				return r, model.ErrInvalid
			}
			scopes[b.Scope] = true
		}
	}
	for i := range r.Conversations {
		item := &r.Conversations[i]
		key := "conversation/" + item.ConversationID
		if !model.ValidID(item.ConversationID) || !model.ValidID(item.Scope) || !model.ValidID(item.PolicyID) || !model.ValidID(item.Jurisdiction) || item.Purpose == "" || len(item.Purpose) > 256 || selected[key] || (item.Command != (model.Command{}) && item.Command != r.Command) || (item.Reason != "" && item.Reason != r.Reason) {
			return r, model.ErrInvalid
		}
		selected[key] = true
		item.Command = r.Command
		item.Reason = r.Reason
		scopes[item.Scope] = true
	}
	sort.Slice(r.Conversations, func(i, j int) bool { return r.Conversations[i].ConversationID < r.Conversations[j].ConversationID })
	for i := range r.Attestations {
		item := &r.Attestations[i]
		key := "attestation/" + item.Kind + "/" + item.AttestationID
		if (item.Kind != "fact" && item.Kind != "memory") || !model.ValidID(item.Scope) || !model.ValidID(item.RecordID) || !model.ValidID(item.VersionID) || !model.ValidID(item.AttestationID) || !model.ValidID(item.PolicyID) || !model.ValidID(item.Jurisdiction) || item.Purpose == "" || len(item.Purpose) > 256 || selected[key] || (item.Command != (model.Command{}) && item.Command != r.Command) || (item.Reason != "" && item.Reason != r.Reason) {
			return r, model.ErrInvalid
		}
		selected[key] = true
		item.Command = r.Command
		item.Reason = r.Reason
		scopes[item.Scope] = true
	}
	sort.Slice(r.Attestations, func(i, j int) bool {
		if r.Attestations[i].Kind != r.Attestations[j].Kind {
			return r.Attestations[i].Kind < r.Attestations[j].Kind
		}
		return r.Attestations[i].AttestationID < r.Attestations[j].AttestationID
	})
	r.Snapshots = append([]model.SnapshotDisposalRequest(nil), r.Snapshots...)
	for i := range r.Snapshots {
		item := &r.Snapshots[i]
		key := "snapshot/" + item.SnapshotID
		if !model.ValidID(item.SnapshotID) || selected[key] || len(item.Bindings) < 1 || len(item.Bindings) > 256 || (item.Command != (model.Command{}) && item.Command != r.Command) || (item.Reason != "" && item.Reason != r.Reason) {
			return r, model.ErrInvalid
		}
		selected[key] = true
		item.Command = r.Command
		item.Reason = r.Reason
		item.Bindings = append([]model.SnapshotRetentionBinding(nil), item.Bindings...)
		sort.Slice(item.Bindings, func(a, b int) bool { return item.Bindings[a].Scope < item.Bindings[b].Scope })
		for j, b := range item.Bindings {
			if !model.ValidID(b.Scope) || !model.ValidID(b.PolicyID) || !model.ValidID(b.Jurisdiction) || (j > 0 && item.Bindings[j-1].Scope == b.Scope) {
				return r, model.ErrInvalid
			}
			scopes[b.Scope] = true
		}
	}
	sort.Slice(r.Snapshots, func(i, j int) bool { return r.Snapshots[i].SnapshotID < r.Snapshots[j].SnapshotID })
	r.Contexts = append([]model.ContextDisposalRequest(nil), r.Contexts...)
	for i := range r.Contexts {
		item := &r.Contexts[i]
		key := "context/" + item.ContextID
		if !model.ValidID(item.ContextID) || selected[key] || len(item.Bindings) < 1 || len(item.Bindings) > 256 || (item.Command != (model.Command{}) && item.Command != r.Command) || (item.Reason != "" && item.Reason != r.Reason) {
			return r, model.ErrInvalid
		}
		selected[key] = true
		item.Command = r.Command
		item.Reason = r.Reason
		item.Bindings = append([]model.ContextRetentionBinding(nil), item.Bindings...)
		sort.Slice(item.Bindings, func(a, b int) bool { return item.Bindings[a].Scope < item.Bindings[b].Scope })
		for j, b := range item.Bindings {
			if !model.ValidID(b.Scope) || !model.ValidID(b.PolicyID) || !model.ValidID(b.Jurisdiction) || (j > 0 && item.Bindings[j-1].Scope == b.Scope) {
				return r, model.ErrInvalid
			}
			scopes[b.Scope] = true
		}
	}
	sort.Slice(r.Contexts, func(i, j int) bool { return r.Contexts[i].ContextID < r.Contexts[j].ContextID })
	r.Calculations = append([]model.CalculationDisposalRequest(nil), r.Calculations...)
	for i := range r.Calculations {
		item := &r.Calculations[i]
		key := "calculation/" + item.CalculationID
		if !model.ValidID(item.CalculationID) || selected[key] || len(item.Bindings) < 1 || len(item.Bindings) > 256 || (item.Command != (model.Command{}) && item.Command != r.Command) || (item.Reason != "" && item.Reason != r.Reason) {
			return r, model.ErrInvalid
		}
		selected[key] = true
		item.Command = r.Command
		item.Reason = r.Reason
		item.Bindings = append([]model.CalculationRetentionBinding(nil), item.Bindings...)
		sort.Slice(item.Bindings, func(a, b int) bool { return item.Bindings[a].Scope < item.Bindings[b].Scope })
		for j, b := range item.Bindings {
			if !model.ValidID(b.Scope) || !model.ValidID(b.PolicyID) || !model.ValidID(b.Jurisdiction) || (j > 0 && item.Bindings[j-1].Scope == b.Scope) {
				return r, model.ErrInvalid
			}
			scopes[b.Scope] = true
		}
	}
	sort.Slice(r.Calculations, func(i, j int) bool { return r.Calculations[i].CalculationID < r.Calculations[j].CalculationID })
	r.Decisions = append([]model.DecisionDisposalRequest(nil), r.Decisions...)
	for i := range r.Decisions {
		item := &r.Decisions[i]
		key := "decision/" + item.DecisionID
		if !model.ValidID(item.DecisionID) || selected[key] || len(item.Bindings) < 1 || len(item.Bindings) > 256 || (item.Command != (model.Command{}) && item.Command != r.Command) || (item.Reason != "" && item.Reason != r.Reason) {
			return r, model.ErrInvalid
		}
		selected[key] = true
		item.Command = r.Command
		item.Reason = r.Reason
		item.Bindings = append([]model.DecisionRetentionBinding(nil), item.Bindings...)
		sort.Slice(item.Bindings, func(a, b int) bool { return item.Bindings[a].Scope < item.Bindings[b].Scope })
		for j, b := range item.Bindings {
			if !model.ValidID(b.Scope) || !model.ValidID(b.PolicyID) || !model.ValidID(b.Jurisdiction) || (j > 0 && item.Bindings[j-1].Scope == b.Scope) {
				return r, model.ErrInvalid
			}
			scopes[b.Scope] = true
		}
	}
	sort.Slice(r.Decisions, func(i, j int) bool { return r.Decisions[i].DecisionID < r.Decisions[j].DecisionID })
	if len(scopes) > 256 || !scopes[r.Scope] {
		return r, model.ErrInvalid
	}
	sort.Slice(r.Versions, func(i, j int) bool {
		a, b := r.Versions[i], r.Versions[j]
		if a.Kind != b.Kind {
			return a.Kind > b.Kind
		} // Memories precede their fact dependencies.
		if a.RecordID != b.RecordID {
			return a.RecordID < b.RecordID
		}
		return a.VersionID < b.VersionID
	})
	sort.Slice(r.Extractions, func(i, j int) bool { return r.Extractions[i].JobID < r.Extractions[j].JobID })
	return r, nil
}
func (s *Service) DisposeBatch(ctx context.Context, c model.Caller, r model.DisposalBatchRequest) (model.CommitResult, error) {
	r, err := normalizeDisposalBatch(r)
	if err != nil {
		return model.CommitResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	decisions := map[string]model.AuthorizationDecision{}
	check := func() error {
		decisions = map[string]model.AuthorizationDecision{}
		for _, scope := range r.Scopes() {
			d, err := s.authorize(ctx, c, scope, "can_dispose", r.Command)
			if err != nil {
				return err
			}
			decisions[scope] = d
		}
		return nil
	}
	if err = check(); err != nil {
		return model.CommitResult{}, err
	}
	if len(r.Evidence) > 0 {
		if err = s.checkDisposalDomain(ctx); err != nil {
			return model.CommitResult{}, err
		}
	}
	return s.mutate(ctx, c, r.Scope, "dispose_batch", "can_dispose", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		batch, ok := tx.(model.DisposalBatchTx)
		if !ok {
			return model.ErrPrecondition
		}
		retention, err := retentionTx(tx)
		if err != nil {
			return err
		}
		batchResult, err := batch.DisposeBatch(ctx, r)
		if err != nil {
			return err
		}
		if len(batchResult.VersionRevisions) != len(r.Versions) || len(batchResult.AttestationRevisions) != len(r.Attestations) {
			return model.ErrPrecondition
		}
		if err = check(); err != nil {
			return err
		}
		batchID := id()
		appendChild := func(event model.RetentionEvent, recordKind, recordID, versionID string, revision int64) error {
			event.ID = id()
			event.BatchID = batchID
			event.Tenant = c.Tenant
			event.Caller = c
			event.Command = r.Command
			if err := retention.StageRetention(ctx, event); err != nil {
				return err
			}
			_, err := tx.Append(ctx, model.Event{ID: id(), EntityID: event.ID, Type: event.Kind, Scope: event.Scope, RecordKind: recordKind, RecordID: recordID, VersionID: versionID, StreamRevision: revision, Caller: c, Command: r.Command, Authorization: decisions[event.Scope]})
			return err
		}
		for i, item := range r.Attestations {
			d := decisions[item.Scope]
			event := model.RetentionEvent{Scope: item.Scope, Kind: "dispose_" + item.Kind + "_attestation", TargetID: item.AttestationID, AttestationDisposal: &item, AdditionalAuthorization: []model.AuthorizationDecision{d}, Limitations: []string{"attestation_identity_retained", "confirmation_evidence_retained", "archives_expire_under_separate_policy", "backups_expire_under_separate_policy"}}
			if err = appendChild(event, item.Kind, item.RecordID, item.VersionID, batchResult.AttestationRevisions[i]); err != nil {
				return err
			}
		}
		for _, item := range r.Snapshots {
			receipts := []model.AuthorizationDecision{}
			for _, b := range item.Bindings {
				receipts = append(receipts, decisions[b.Scope])
			}
			event := model.RetentionEvent{Scope: item.Bindings[0].Scope, Kind: "dispose_snapshot", TargetID: item.SnapshotID, SnapshotDisposal: &item, AdditionalAuthorization: receipts, Limitations: []string{"structural_metadata_retained", "unselected_sources_retained", "archives_expire_under_separate_policy", "backups_expire_under_separate_policy"}}
			if err = appendChild(event, "", "", "", 0); err != nil {
				return err
			}
		}
		for _, item := range r.Contexts {
			receipts := []model.AuthorizationDecision{}
			for _, b := range item.Bindings {
				receipts = append(receipts, decisions[b.Scope])
			}
			event := model.RetentionEvent{Scope: item.Bindings[0].Scope, Kind: "dispose_context", TargetID: item.ContextID, ContextDisposal: &item, AdditionalAuthorization: receipts, Limitations: []string{"structural_metadata_retained", "unselected_sources_retained", "archives_expire_under_separate_policy", "backups_expire_under_separate_policy"}}
			if err = appendChild(event, "", "", "", 0); err != nil {
				return err
			}
		}
		for _, item := range r.Calculations {
			receipts := []model.AuthorizationDecision{}
			for _, b := range item.Bindings {
				receipts = append(receipts, decisions[b.Scope])
			}
			event := model.RetentionEvent{Scope: item.Bindings[0].Scope, Kind: "dispose_calculation", TargetID: item.CalculationID, CalculationDisposal: &item, AdditionalAuthorization: receipts, Limitations: []string{"structural_metadata_retained", "unselected_sources_retained", "archives_expire_under_separate_policy", "backups_expire_under_separate_policy"}}
			if err = appendChild(event, "", "", "", 0); err != nil {
				return err
			}
		}
		for _, item := range r.Decisions {
			receipts := []model.AuthorizationDecision{}
			for _, b := range item.Bindings {
				receipts = append(receipts, decisions[b.Scope])
			}
			event := model.RetentionEvent{Scope: item.Bindings[0].Scope, Kind: "dispose_decision", TargetID: item.DecisionID, DecisionDisposal: &item, AdditionalAuthorization: receipts, Limitations: []string{"structural_metadata_retained", "unselected_sources_retained", "archives_expire_under_separate_policy", "backups_expire_under_separate_policy"}}
			if err = appendChild(event, "", "", "", 0); err != nil {
				return err
			}
		}
		for _, e := range r.Extractions {
			receipts := []model.AuthorizationDecision{}
			for _, b := range e.Bindings {
				receipts = append(receipts, decisions[b.Scope])
			}
			event := model.RetentionEvent{Scope: e.Bindings[0].Scope, Kind: "dispose_extraction", TargetID: e.JobID, ExtractionDisposal: &e, AdditionalAuthorization: receipts, Limitations: []string{"structural_metadata_retained", "unselected_sources_retained", "archives_expire_under_separate_policy", "backups_expire_under_separate_policy"}}
			if err = appendChild(event, "", "", "", 0); err != nil {
				return err
			}
		}
		for _, item := range r.Conversations {
			d := decisions[item.Scope]
			event := model.RetentionEvent{Scope: item.Scope, Kind: "dispose_conversation", TargetID: item.ConversationID, ConversationDisposal: &item, AdditionalAuthorization: []model.AuthorizationDecision{d}, Limitations: []string{"source_evidence_retained_under_separate_policy", "exports_and_archives_expire_under_separate_policy", "backups_expire_under_separate_policy"}}
			if err = appendChild(event, "", "", "", 0); err != nil {
				return err
			}
		}
		for i, v := range r.Versions {
			if batchResult.VersionRevisions[i] < 1 {
				return model.ErrPrecondition
			}
			receipts := []model.AuthorizationDecision{}
			for _, b := range v.Bindings {
				receipts = append(receipts, decisions[b.Scope])
			}
			event := model.RetentionEvent{Scope: v.Scope, Kind: "dispose_" + v.Kind + "_payload", TargetID: v.VersionID, VersionDisposal: &v, AdditionalAuthorization: receipts, Limitations: []string{"structural_metadata_retained", "projection_cleanup_requires_acknowledgements", "source_evidence_retained", "archives_expire_under_separate_policy", "backups_expire_under_separate_policy"}}
			if err = appendChild(event, v.Kind, v.RecordID, v.VersionID, batchResult.VersionRevisions[i]); err != nil {
				return err
			}
		}
		for _, item := range r.Evidence {
			d := decisions[item.Scope]
			event := model.RetentionEvent{Scope: item.Scope, Kind: "dispose_evidence", TargetID: item.EvidenceID, Disposal: &item, AdditionalAuthorization: []model.AuthorizationDecision{d}, Limitations: []string{"structural_metadata_retained", "backups_expire_under_separate_policy", "archives_expire_under_separate_policy", "external_copies_require_separate_disposal"}}
			if err = appendChild(event, "", "", "", 0); err != nil {
				return err
			}
		}
		receipts := []model.AuthorizationDecision{}
		for _, scope := range r.Scopes() {
			receipts = append(receipts, decisions[scope])
		}
		out.ID = batchID
		return retention.StageRetention(ctx, model.RetentionEvent{ID: batchID, Tenant: c.Tenant, Scope: r.Scope, Kind: "dispose_batch", TargetID: batchID, Caller: c, Command: r.Command, DisposalBatch: &r, AdditionalAuthorization: receipts, Limitations: []string{"unselected_artifacts_retained", "projection_cleanup_requires_acknowledgements", "archives_expire_under_separate_policy", "backups_expire_under_separate_policy"}})
	}, mutationCheck{onReceipt: check, onCommit: check})
}
