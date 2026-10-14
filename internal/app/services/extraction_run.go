package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/caxqueiroz/facts_engine/model"
	"math/rand/v2"
	"slices"
	"sort"
	"time"
)

func extractionRunTx(tx model.Tx) (model.ExtractionRunTx, error) {
	t, ok := tx.(model.ExtractionRunTx)
	if !ok {
		return nil, model.ErrPrecondition
	}
	return t, nil
}
func (s *Service) extractionAuthorizations(ctx context.Context, c model.Caller, j model.ExtractionJob) ([]model.AuthorizationDecision, error) {
	receipts := []model.AuthorizationDecision{}
	for _, caller := range []model.Caller{j.Caller, c} {
		for _, permission := range []string{"can_extract", "can_propose", "can_read_evidence"} {
			d, err := s.authorize(ctx, caller, j.Request.Scope, permission, j.Request.Command)
			if err != nil {
				return nil, err
			}
			receipts = append(receipts, d)
		}
		if j.Request.Refresh != nil || len(j.Request.MemoryTargets) > 0 {
			checks, err := s.extractionContentAuthorization(ctx, caller, j)
			if err != nil {
				return nil, err
			}
			receipts = append(receipts, checks...)
		}
	}
	return receipts, nil
}
func (s *Service) loadExtractionJob(ctx context.Context, c model.Caller, q model.JobQuery) (model.ExtractionJob, model.JobStatus, error) {
	status, err := s.GetJob(ctx, c, q)
	if err != nil {
		return model.ExtractionJob{}, status, err
	}
	var j model.ExtractionJob
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := extractionTx(tx)
		if err != nil {
			return err
		}
		j, err = t.ExtractionJob(ctx, q.ID)
		return err
	})
	if err != nil {
		return j, status, err
	}
	if j.Tenant != c.Tenant || j.Request.Scope != q.Scope || model.ExtractionProfileDigest(j.Profile) != j.ProfileDigest {
		return j, status, model.ErrPrecondition
	}
	return j, status, nil
}
func (s *Service) RunExtraction(ctx context.Context, c model.Caller, r model.ExtractionRunRequest) (model.JobStatus, error) {
	if !c.Service || !model.ValidID(r.Key) {
		return model.JobStatus{}, model.ErrInvalid
	}
	job, status, err := s.loadExtractionJob(ctx, c, model.JobQuery{ID: r.ID, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil {
		return model.JobStatus{}, err
	}
	if status.State == "completed" || status.State == "failed" {
		return status, nil
	}
	if r.Purpose != job.Request.Purpose {
		return model.JobStatus{}, model.ErrInvalid
	}
	approved, ok := s.extractors[job.Profile.ID]
	if !ok || model.ExtractionProfileDigest(approved.Profile) != job.ProfileDigest {
		return model.JobStatus{}, model.ErrPrecondition
	}
	// The requesting actor is the recorded initiator of service execution. It is
	// taken from the authenticated retained job, never a provider candidate.
	c.Initiator = job.Caller.Human()
	receipts, err := s.extractionAuthorizations(ctx, c, job)
	if err != nil {
		return model.JobStatus{}, err
	}
	if status.State == "running" && status.Attempts >= status.MaxAttempts && status.LeaseUntil != nil && !status.LeaseUntil.After(time.Now()) {
		return s.expireExtraction(ctx, c, r, status, receipts)
	}
	// Also gate pre-registry queued jobs before disclosing input to a provider.
	if err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		if err := tx.RequireScope(ctx, job.Request.Scope); err != nil {
			return err
		}
		for _, subject := range job.Request.Subjects {
			if err := tx.RequireSubject(ctx, job.Request.Scope, subject); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return model.JobStatus{}, err
	}
	attempt := model.ExtractionAttempt{ContentScopes: job.ContentScopes, RefreshScopes: job.RefreshScopes, ID: id(), Tenant: c.Tenant, Scope: r.Scope, JobID: job.ID, OwnerNonce: id(), ProfileDigest: job.ProfileDigest, Caller: c, Authorization: receipts}
	var input model.ExtractionInput
	inputLimit := false
	if job.Request.Refresh != nil {
		refresh, err := s.buildMemoryRefresh(ctx, c, job)
		inputLimit = errors.Is(err, errRefreshInputLimit)
		if err != nil && !inputLimit {
			return model.JobStatus{}, err
		}
		input = model.ExtractionInput{Schema: "memory-refresh-request-v1", Tenant: c.Tenant, JobID: job.ID, AttemptID: attempt.ID, Profile: job.Profile, Refresh: refresh, RedactionVersion: "original-utf8-v1"}
	} else {
		conversation, err := s.GetConversation(ctx, c, model.ConversationQuery{ID: job.Request.ConversationID, Scope: r.Scope, Purpose: r.Purpose})
		if err != nil {
			return model.JobStatus{}, err
		}
		input = model.ExtractionInput{Schema: "facts-extraction-input-v1", Tenant: c.Tenant, JobID: job.ID, AttemptID: attempt.ID, ConversationID: conversation.ID, ConversationDigest: conversation.Digest, Timezone: conversation.Request.Timezone, Profile: job.Profile, Subjects: job.Request.Subjects, Predicates: job.Predicates, Messages: []model.ExtractionInputMessage{}, OmittedRanges: []string{}, RedactionVersion: "original-utf8-v1"}
		if len(job.Request.MemoryTargets) > 0 {
			input.Schema = "memory-extraction-input-v1"
			input.MemoryTargets = job.Request.MemoryTargets
			input.MemoryTypes = job.MemoryTypes
			input.DependencyVersions = job.DependencyVersions
			for _, v := range job.DependencyVersions {
				ids := []string{}
				for _, span := range v.Proposal.Sources {
					ids = append(ids, span.EvidenceID)
				}
				if _, err = s.loadEvidence(ctx, c, v.Proposal.Key.Scope, ids); err != nil {
					return model.JobStatus{}, err
				}
			}
		}
		ranges, excluded, _, err := planExtractionRanges(conversation, job.Request.InputRanges, job.Profile.InputSelectionVersion)
		if err != nil {
			return model.JobStatus{}, err
		}
		if len(job.Request.InputRanges) > 0 {
			input.InputSelectionVersion = job.Profile.InputSelectionVersion
			input.ExcludedRanges = excluded
		}
		for i, m := range conversation.Request.Messages {
			if len(ranges[i]) == 0 {
				continue
			}
			content, err := s.GetEvidenceContent(ctx, c, model.EvidenceQuery{ID: conversation.EvidenceIDs[i], Scope: r.Scope, Purpose: r.Purpose})
			if err != nil {
				return model.JobStatus{}, err
			}
			for _, selected := range ranges[i] {
				message, err := extractionRangeMessage(m, selected, content.Bytes)
				if err != nil {
					return model.JobStatus{}, err
				}
				input.Messages = append(input.Messages, message)
			}
		}

	}
	attempt.RequestBytes, err = json.Marshal(input)
	if err != nil {
		return model.JobStatus{}, err
	}
	if inputLimit || len(attempt.RequestBytes) > job.Profile.MaxInputBytes {
		_, err = s.mutate(ctx, c, r.Scope, "reject_extraction_input", "can_extract", model.Command{Key: model.Digest([]string{r.Key, "input-limit"}), Purpose: r.Purpose}, r, func(tx model.Tx, out *model.CommitResult) error {
			t, err := extractionRunTx(tx)
			if err != nil {
				return err
			}
			out.ID = job.ID
			return t.RejectExtractionInput(ctx, job.ID)
		})
		if err != nil {
			return model.JobStatus{}, err
		}
		return s.GetJob(ctx, c, model.JobQuery{ID: job.ID, Scope: r.Scope, Purpose: r.Purpose})
	}
	attempt.RequestDigest = model.BytesDigest(attempt.RequestBytes)
	started, err := s.mutate(ctx, c, r.Scope, "begin_extraction", "can_extract", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := extractionRunTx(tx)
		if err != nil {
			return err
		}
		claim, err := t.BeginExtraction(ctx, job.ID, attempt.ID, s.extractionLimits.MaxConcurrentPerTenant, s.extractionLimits.DailyTokenBudget, job.Profile.MaxTokensPerAttempt, job.Profile.Timeout+30*time.Second, model.ExtractionCostBudget{Unit: job.Profile.CostUnit, DailyLimit: s.extractionLimits.DailyCostBudgetUnits, PerAttempt: job.Profile.MaxCostUnitsPerAttempt})
		if err != nil {
			return err
		}
		for _, d := range receipts {
			if time.Since(d.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		attempt.Reservation = &claim.Reservation
		attempt.Fence = claim.Fence
		attempt.Number = claim.Attempts
		attempt.LeaseUntil = *claim.LeaseUntil
		if err = t.StageExtractionAttempt(ctx, attempt); err != nil {
			return err
		}
		out.ID = attempt.ID
		out.ContentDigest = attempt.RequestDigest
		return nil
	})
	if err != nil {
		return model.JobStatus{}, err
	}
	var retained model.ExtractionAttempt
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := extractionRunTx(tx)
		if err != nil {
			return err
		}
		retained, err = t.ExtractionAttempt(ctx, started.ID)
		return err
	})
	if err != nil {
		return model.JobStatus{}, err
	}
	if retained.OwnerNonce != attempt.OwnerNonce {
		return model.JobStatus{}, model.ErrUnavailable
	}
	// The request must be durably retained before any provider call. A lost begin
	// response cannot cause two callers with the same command key to invoke it.
	if !retained.LeaseUntil.After(time.Now().Add(time.Second)) {
		return model.JobStatus{}, model.ErrUnavailable
	}
	providerCtx, cancel := context.WithTimeout(ctx, min(job.Profile.Timeout, time.Until(retained.LeaseUntil)-time.Second))
	response, providerErr := approved.Provider.Extract(providerCtx, append([]byte(nil), retained.RequestBytes...))
	if providerErr == nil {
		providerErr = providerCtx.Err()
	}
	cancel()
	result := model.ExtractionResult{ContentScopes: job.ContentScopes, RefreshScopes: job.RefreshScopes, AttemptID: retained.ID, JobID: job.ID, Tenant: c.Tenant, Scope: r.Scope, Outcome: "completed", ModelIdentity: response.ModelIdentity, RequestDigest: retained.RequestDigest, ResponseDigest: model.BytesDigest(response.Body), ResponseComplete: true, InputTokens: response.InputTokens, OutputTokens: response.OutputTokens, Issues: []model.ExtractionIssue{}, Relations: []model.ExtractionRelation{}, Proposals: []model.ExtractedProposal{}}
	if len(response.Body) > job.Profile.MaxOutputBytes {
		result.ResponseComplete = false
		result.Outcome = "response_limit"
		result.ResponseBytes = append([]byte(nil), response.Body[:job.Profile.MaxOutputBytes]...)
	} else {
		result.ResponseBytes = append([]byte(nil), response.Body...)
	}
	var output model.ExtractionOutput
	var refreshOutput model.MemoryRefreshOutput
	var memoryOutput model.MemoryExtractionOutput
	if providerErr != nil {
		result.Outcome = "provider_unavailable"
	} else if result.Outcome == "completed" {
		if response.ModelIdentity == "" || len(response.ModelIdentity) > 256 || response.InputTokens < 0 || response.OutputTokens < 0 || response.InputTokens > job.Profile.MaxTokensPerAttempt || response.OutputTokens > job.Profile.MaxTokensPerAttempt-response.InputTokens {
			result.Outcome = "invalid_model_metadata"
		} else {
			if job.Request.Refresh != nil {
				refreshOutput, err = parseMemoryRefresh(response.Body)
			} else if len(job.Request.MemoryTargets) > 0 {
				memoryOutput, err = parseMemoryExtraction(response.Body, job.Profile.MaxCandidates)
			} else {
				output, err = parseExtraction(response.Body, job.Profile.MaxCandidates)
			}
			if err != nil {
				result.Outcome = "invalid_response"
			}
		}
	}
	receipts, err = s.extractionAuthorizations(ctx, c, job)
	if err != nil {
		if !errors.Is(err, model.ErrDenied) {
			return model.JobStatus{}, err
		}
		receipts = nil
		for _, permission := range []string{"can_extract", "can_read_evidence"} {
			d, checkErr := s.authorize(ctx, c, r.Scope, permission, r.Command)
			if checkErr != nil {
				return model.JobStatus{}, checkErr
			}
			receipts = append(receipts, d)
		}
		result.Outcome = "authorization_changed"
	}
	result.Authorization = receipts
	complete := model.Command{Key: retained.ID + "-complete", Purpose: r.Purpose, CorrelationID: r.CorrelationID}
	_, err = s.mutate(ctx, c, r.Scope, "complete_extraction", "can_extract", complete, struct {
		AttemptID, Digest, Outcome, Model string
		InputTokens, OutputTokens         int64
	}{retained.ID, result.ResponseDigest, result.Outcome, result.ModelIdentity, result.InputTokens, result.OutputTokens}, func(tx model.Tx, out *model.CommitResult) error {
		t, err := extractionRunTx(tx)
		if err != nil {
			return err
		}
		state := "completed"
		failure := ""
		if result.Outcome != "completed" {
			state = "failed"
			failure = result.Outcome
		}
		if result.Outcome == "provider_unavailable" {
			state = "pending"
		}
		if err = t.FinishExtraction(ctx, job.ID, retained.ID, retained.Fence, state, failure, extractionBackoff(job.Profile.RetryDelay, retained.Number)); err != nil {
			return err
		}
		for _, d := range receipts {
			if time.Since(d.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		result.Issues = nil
		result.Relations = nil
		result.Proposals = nil
		if result.Outcome == "completed" {
			if job.Request.Refresh != nil {
				err = s.appendRefreshedMemory(ctx, t, c, job, *input.Refresh, refreshOutput, retained.ID, receipts, &result)
			} else if len(job.Request.MemoryTargets) > 0 {
				err = s.appendExtractedMemories(ctx, t, c, job, input, memoryOutput, retained.ID, receipts, &result)
			} else {
				err = s.appendExtractedProposals(ctx, t, c, job, input, output, retained.ID, receipts, &result)
			}
			if err != nil {
				return err
			}
		}
		if err = t.StageExtractionResult(ctx, result); err != nil {
			return err
		}
		out.ID = job.ID
		out.ContentDigest = result.ResponseDigest
		return nil
	})
	if err != nil {
		return model.JobStatus{}, err
	}
	return s.GetJob(ctx, c, model.JobQuery{ID: job.ID, Scope: r.Scope, Purpose: r.Purpose})
}
func (s *Service) appendExtractedProposals(ctx context.Context, t model.ExtractionRunTx, c model.Caller, job model.ExtractionJob, input model.ExtractionInput, output model.ExtractionOutput, attempt string, auth []model.AuthorizationDecision, result *model.ExtractionResult) error {
	counts := map[model.FactKey]int{}
	key := func(candidate model.ExtractionCandidate) model.FactKey {
		return model.FactKey{Scope: job.Request.Scope, Subject: candidate.Subject, Predicate: candidate.Predicate, Discriminator: candidate.Discriminator}
	}
	for _, candidate := range output.Candidates {
		counts[key(candidate)]++
	}
	order := make([]int, len(output.Candidates))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return model.Digest(key(output.Candidates[order[i]])) < model.Digest(key(output.Candidates[order[j]]))
	})
	// Lock all valid record keys before appending any event/tenant revision.
	records := map[model.FactKey]model.Record{}
	for _, ordinal := range order {
		candidate := output.Candidates[ordinal]
		code := candidateIssue(candidate, input)
		if counts[key(candidate)] > 1 {
			code = "multiple_candidates_for_key"
		}
		if code != "" {
			result.Issues = append(result.Issues, model.ExtractionIssue{Candidate: ordinal, Code: code})
			continue
		}
		rec, err := t.LockRecord(ctx, key(candidate))
		if err != nil {
			return err
		}
		records[key(candidate)] = rec
	}
	var proposeAuth model.AuthorizationDecision
	for _, d := range auth {
		if d.Request.Caller == c && d.Request.Permission == "can_propose" {
			proposeAuth = d
			break
		}
	}
	if !proposeAuth.Allowed {
		return model.ErrDenied
	}
	revision, err := t.Revision(ctx)
	if err != nil {
		return err
	}
	for _, ordinal := range order {
		candidate := output.Candidates[ordinal]
		rec, ok := records[key(candidate)]
		if !ok {
			continue
		}
		active, err := t.ActivePredicate(ctx, job.Request.Scope, candidate.Predicate)
		if err != nil {
			return err
		}
		if active != candidate.SchemaVersion {
			result.Issues = append(result.Issues, model.ExtractionIssue{Candidate: ordinal, Code: "schema_changed"})
			continue
		}
		definition, err := t.Predicate(ctx, job.Request.Scope, candidate.Predicate, candidate.SchemaVersion)
		if err != nil {
			return err
		}
		policy, err := t.Policy(ctx, job.Request.Scope, definition.PolicyID)
		if err != nil {
			return err
		}
		allowed := true
		for _, span := range candidate.Sources {
			e, err := t.Evidence(ctx, span.EvidenceID)
			if err != nil {
				return err
			}
			if !slices.Contains(policy.SourceClasses, e.SourceClass) {
				allowed = false
			}
		}
		if !allowed {
			result.Issues = append(result.Issues, model.ExtractionIssue{Candidate: ordinal, Code: "source_authority_not_allowed"})
			continue
		}
		versions, err := t.Versions(ctx, rec.ID, revision)
		if err != nil {
			return err
		}
		accepted, err := t.Acceptances(ctx, rec.ID, revision)
		if err != nil {
			return err
		}
		duplicate := false
		for _, v := range versions {
			if model.Digest(v.Proposal.Value) == model.Digest(candidate.Value) && model.Digest(v.Proposal.Valid) == model.Digest(candidate.Valid) && v.Proposal.SchemaVersion == candidate.SchemaVersion && model.Digest(v.Proposal.Sources) == model.Digest(candidate.Sources) {
				duplicate = true
				result.Relations = append(result.Relations, model.ExtractionRelation{Candidate: ordinal, RecordID: rec.ID, VersionID: v.ID, Kind: "duplicate"})
			}
			for _, a := range accepted {
				if a.VersionID == v.ID && a.State != "retracted" && a.Valid.Overlaps(candidate.Valid) && model.Digest(v.Proposal.Value) != model.Digest(candidate.Value) {
					result.Relations = append(result.Relations, model.ExtractionRelation{Candidate: ordinal, RecordID: rec.ID, VersionID: v.ID, Kind: "overlapping_different_value"})
				}
			}
		}
		if duplicate {
			continue
		}
		request := model.FactProposal{Command: model.Command{Key: fmt.Sprintf("%s-%d", attempt, ordinal), Purpose: job.Request.Purpose, CorrelationID: job.Request.CorrelationID}, Key: key(candidate), SchemaVersion: candidate.SchemaVersion, Value: candidate.Value, Valid: candidate.Valid, Sources: candidate.Sources, ExpectedRevision: rec.Revision}
		var proposed model.CommitResult
		if err = s.stageFactProposal(ctx, t, c, request, attempt, &proposed); err != nil {
			return err
		}
		if _, err = t.Append(ctx, model.Event{ID: id(), EntityID: proposed.ID, Type: "propose_fact", Scope: job.Request.Scope, RecordID: proposed.RecordID, VersionID: proposed.VersionID, StreamRevision: proposed.StreamRevision, Caller: c, Command: request.Command, Authorization: proposeAuth}); err != nil {
			return err
		}
		result.Proposals = append(result.Proposals, model.ExtractedProposal{Candidate: ordinal, RecordID: proposed.RecordID, VersionID: proposed.VersionID})
	}
	sort.Slice(result.Issues, func(i, j int) bool { return result.Issues[i].Candidate < result.Issues[j].Candidate })
	sort.Slice(result.Proposals, func(i, j int) bool { return result.Proposals[i].Candidate < result.Proposals[j].Candidate })
	return nil
}
func (s *Service) GetExtractionRun(ctx context.Context, c model.Caller, q model.ExtractionRunQuery) (model.ExtractionRun, error) {
	if !model.ValidID(q.ID) {
		return model.ExtractionRun{}, model.ErrInvalid
	}
	for _, permission := range []string{"can_extract", "can_read_evidence"} {
		if _, err := s.authorize(ctx, c, q.Scope, permission, model.Command{Purpose: q.Purpose}); err != nil {
			return model.ExtractionRun{}, err
		}
	}
	var run model.ExtractionRun
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := extractionRunTx(tx)
		if err != nil {
			return err
		}
		run.Attempt, err = t.ExtractionAttempt(ctx, q.ID)
		if err != nil {
			return err
		}
		result, err := t.ExtractionResult(ctx, q.ID)
		if errors.Is(err, model.ErrNotFound) {
			return nil
		}
		if err == nil {
			run.Result = &result
		}
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		return model.ExtractionRun{}, model.ErrDenied
	}
	if err != nil {
		return model.ExtractionRun{}, err
	}
	if run.Attempt.Tenant != c.Tenant || run.Attempt.Scope != q.Scope {
		return model.ExtractionRun{}, model.ErrDenied
	}
	if model.BytesDigest(run.Attempt.RequestBytes) != run.Attempt.RequestDigest {
		return model.ExtractionRun{}, model.ErrPrecondition
	}
	if run.Result != nil && (run.Result.RequestDigest != run.Attempt.RequestDigest || (run.Result.ResponseComplete && model.BytesDigest(run.Result.ResponseBytes) != run.Result.ResponseDigest)) {
		return model.ExtractionRun{}, model.ErrPrecondition
	}
	for _, permission := range []string{"can_extract", "can_read_evidence"} {
		if _, err = s.authorize(ctx, c, q.Scope, permission, model.Command{Purpose: q.Purpose}); err != nil {
			return model.ExtractionRun{}, err
		}
	}
	job, _, err := s.loadExtractionJob(ctx, c, model.JobQuery{ID: run.Attempt.JobID, Scope: q.Scope, Purpose: q.Purpose})
	if err != nil {
		return model.ExtractionRun{}, err
	}
	if job.Request.Refresh != nil || len(job.Request.MemoryTargets) > 0 {
		job.Request.Purpose = q.Purpose
		if _, err = s.extractionContentAuthorization(ctx, c, job); err != nil {
			return model.ExtractionRun{}, err
		}
	}
	run.Attempt.OwnerNonce = ""
	return run, nil
}

func extractionBackoff(base time.Duration, attempt int) time.Duration {
	for i := 1; i < attempt && base < time.Hour; i++ {
		base = min(time.Hour, base*2)
	}
	return min(time.Hour, base+time.Duration(rand.Int64N(int64(base/4)+1)))
}
func (s *Service) expireExtraction(ctx context.Context, c model.Caller, r model.ExtractionRunRequest, status model.JobStatus, auth []model.AuthorizationDecision) (model.JobStatus, error) {
	cmd := model.Command{Key: model.Digest([]string{r.Key, "lease-exhausted"}), Purpose: r.Purpose}
	_, err := s.mutate(ctx, c, r.Scope, "expire_extraction", "can_extract", cmd, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := extractionRunTx(tx)
		if err != nil {
			return err
		}
		if err = t.FailExpiredExtraction(ctx, r.ID, status.Fence); err != nil {
			return err
		}
		attempt, err := t.ExtractionAttempt(ctx, status.AttemptID)
		if err != nil {
			return err
		}
		if err = t.StageExtractionResult(ctx, model.ExtractionResult{ContentScopes: attempt.ContentScopes, RefreshScopes: attempt.RefreshScopes, AttemptID: attempt.ID, JobID: r.ID, Tenant: c.Tenant, Scope: r.Scope, Outcome: "lease_exhausted_response_unavailable", RequestDigest: attempt.RequestDigest, ResponseComplete: false, Authorization: auth}); err != nil {
			return err
		}
		out.ID = r.ID
		return nil
	})
	if err != nil {
		return model.JobStatus{}, err
	}
	return s.GetJob(ctx, c, model.JobQuery{ID: r.ID, Scope: r.Scope, Purpose: r.Purpose})
}
