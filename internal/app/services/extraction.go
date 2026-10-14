package services

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

func configureExtraction(s *Service, o model.ExtractionOptions) error {
	if len(o.Profiles) == 0 {
		if o.MaxConcurrentPerTenant != 0 || o.DailyTokenBudget != 0 {
			return model.ErrInvalid
		}
		return nil
	}
	if len(o.Profiles) > 32 || o.MaxConcurrentPerTenant < 1 || o.MaxConcurrentPerTenant > 32 || o.DailyTokenBudget < 1 {
		return model.ErrInvalid
	}
	s.extractors = map[string]model.ApprovedExtractor{}
	for _, approved := range o.Profiles {
		p := approved.Profile
		if approved.Provider == nil || !model.ValidID(p.ID) || !model.ValidID(p.ProviderID) || !model.ValidID(p.RequestedModel) || !model.ValidID(p.PromptVersion) || (p.ParserVersion != "facts-extraction-json-v1" && p.ParserVersion != "memory-refresh-json-v1" && p.ParserVersion != "memory-extraction-json-v1") || !model.ValidID(p.ApplicationBuild) || len(p.Prompt) == 0 || len(p.Prompt) > 64<<10 || model.BytesDigest(p.Prompt) != p.PromptDigest || len(p.Parameters) > 16<<10 || !json.Valid(p.Parameters) || p.Timeout < time.Second || p.Timeout > 2*time.Minute || p.MaxInputBytes < 1 || p.MaxInputBytes > 16<<20 || p.MaxOutputBytes < 1 || p.MaxOutputBytes > 2<<20 || p.MaxCandidates < 1 || p.MaxCandidates > 64 || p.MaxAttempts < 1 || p.MaxAttempts > 10 || p.RetryDelay < time.Second || p.RetryDelay > time.Hour || p.MaxTokensPerAttempt < 1 || p.MaxTokensPerAttempt > o.DailyTokenBudget {
			return model.ErrInvalid
		}
		if _, exists := s.extractors[p.ID]; exists {
			return model.ErrInvalid
		}
		p.Prompt = append([]byte(nil), p.Prompt...)
		p.Parameters = append(json.RawMessage(nil), p.Parameters...)
		approved.Profile = p
		s.extractors[p.ID] = approved
	}
	s.extractionLimits = model.ExtractionOptions{MaxConcurrentPerTenant: o.MaxConcurrentPerTenant, DailyTokenBudget: o.DailyTokenBudget}
	return nil
}
func extractionTx(tx model.Tx) (model.ExtractionTx, error) {
	t, ok := tx.(model.ExtractionTx)
	if !ok {
		return nil, model.ErrPrecondition
	}
	return t, nil
}
func (s *Service) StartExtraction(ctx context.Context, c model.Caller, r model.ExtractionRequest) (model.JobStatus, error) {
	if r.Refresh != nil || r.RefreshTrigger != nil || !model.ValidID(r.Key) || !model.ValidID(r.ConversationID) || !model.ValidID(r.ProfileID) {
		return model.JobStatus{}, model.ErrInvalid
	}
	memoryMode := len(r.MemoryTargets) > 0
	if memoryMode {
		if len(r.Subjects) > 0 || len(r.Predicates) > 0 || validateMemoryExtractionTargets(r.MemoryTargets) != nil {
			return model.JobStatus{}, model.ErrInvalid
		}
	} else if len(r.Subjects) == 0 || len(r.Subjects) > 64 || len(r.Predicates) == 0 || len(r.Predicates) > 64 {
		return model.JobStatus{}, model.ErrInvalid
	}
	subjects := map[string]bool{}
	for _, subject := range r.Subjects {
		if !model.ValidID(subject) || subjects[subject] {
			return model.JobStatus{}, model.ErrInvalid
		}
		subjects[subject] = true
	}
	predicates := map[string]bool{}
	for _, p := range r.Predicates {
		if !model.ValidID(p.Predicate) || !model.ValidID(p.SchemaVersion) || predicates[p.Predicate] {
			return model.JobStatus{}, model.ErrInvalid
		}
		predicates[p.Predicate] = true
	}
	receipt, err := s.authorize(ctx, c, r.Scope, "can_extract", r.Command)
	if err != nil {
		return model.JobStatus{}, err
	}
	approved, ok := s.extractors[r.ProfileID]
	parser := "facts-extraction-json-v1"
	if memoryMode {
		parser = "memory-extraction-json-v1"
	}
	if !ok || approved.Profile.ParserVersion != parser {
		return model.JobStatus{}, model.ErrPrecondition
	}
	conversation, err := s.GetConversation(ctx, c, model.ConversationQuery{ID: r.ConversationID, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil {
		return model.JobStatus{}, err
	}
	evidenceAuth, err := s.authorize(ctx, c, r.Scope, "can_read_evidence", r.Command)
	if err != nil {
		return model.JobStatus{}, err
	}
	var inputSize int64
	for _, m := range conversation.Request.Messages {
		inputSize += m.Object.Size
	}
	if inputSize > int64(approved.Profile.MaxInputBytes) {
		return model.JobStatus{}, model.ErrPrecondition
	}
	job := model.ExtractionJob{ID: id(), Tenant: c.Tenant, Caller: c, Request: r, Profile: approved.Profile, ProfileDigest: model.ExtractionProfileDigest(approved.Profile), Authorization: []model.AuthorizationDecision{receipt, evidenceAuth}}
	if memoryMode {
		job.ContentScopes = memoryExtractionScopes(r)
		checks, err := s.extractionContentAuthorization(ctx, c, job)
		if err != nil {
			return model.JobStatus{}, err
		}
		job.Authorization = append(job.Authorization, checks...)
	}
	result, err := s.mutate(ctx, c, r.Scope, "start_extraction", "can_extract", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := extractionTx(tx)
		if err != nil {
			return err
		}
		for _, subject := range r.Subjects {
			if err := tx.RequireSubject(ctx, r.Scope, subject); err != nil {
				return err
			}
		}
		job.Predicates = nil
		for _, binding := range r.Predicates {
			active, err := tx.ActivePredicate(ctx, r.Scope, binding.Predicate)
			if err != nil {
				return err
			}
			if active != binding.SchemaVersion {
				return model.ErrPrecondition
			}
			p, err := tx.Predicate(ctx, r.Scope, binding.Predicate, binding.SchemaVersion)
			if err != nil {
				return err
			}
			job.Predicates = append(job.Predicates, p)
		}
		if memoryMode {
			if err = s.bindMemoryExtraction(ctx, tx, &job); err != nil {
				return err
			}
		}
		for _, auth := range job.Authorization {
			if time.Since(auth.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		if err = t.StageExtractionJob(ctx, job); err != nil {
			return err
		}
		out.ID = job.ID
		out.ContentDigest = job.ProfileDigest
		return nil
	})
	if err != nil {
		return model.JobStatus{}, err
	}
	return s.GetJob(ctx, c, model.JobQuery{ID: result.ID, Scope: r.Scope, Purpose: r.Purpose})
}
func (s *Service) GetJob(ctx context.Context, c model.Caller, q model.JobQuery) (model.JobStatus, error) {
	if !model.ValidID(q.ID) {
		return model.JobStatus{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, q.Scope, "can_extract", model.Command{Purpose: q.Purpose}); err != nil {
		return model.JobStatus{}, err
	}
	var status model.JobStatus
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := extractionTx(tx)
		if err != nil {
			return err
		}
		status, err = t.ExtractionJobStatus(ctx, q.ID)
		if err != nil {
			return err
		}
		job, err := t.ExtractionJob(ctx, q.ID)
		if err != nil {
			return err
		}
		status.Purpose = job.Request.Purpose
		if job.Request.Refresh != nil {
			status.Kind = "memory_refresh"
		} else if len(job.Request.MemoryTargets) > 0 {
			status.Kind = "memory_extraction"
		}
		return nil
	})
	if errors.Is(err, model.ErrNotFound) {
		return model.JobStatus{}, model.ErrDenied
	}
	if err != nil {
		return model.JobStatus{}, err
	}
	if status.Scope != q.Scope {
		return model.JobStatus{}, model.ErrDenied
	}
	return status, nil
}
func (s *Service) RetryJob(ctx context.Context, c model.Caller, r model.JobRetryRequest) (model.JobStatus, error) {
	if !model.ValidID(r.ID) || r.ExpectedFence < 0 {
		return model.JobStatus{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, r.Scope, "can_operate", r.Command); err != nil {
		return model.JobStatus{}, err
	}
	if _, err := s.GetJob(ctx, c, model.JobQuery{ID: r.ID, Scope: r.Scope, Purpose: r.Purpose}); err != nil {
		return model.JobStatus{}, err
	}
	_, err := s.mutate(ctx, c, r.Scope, "retry_extraction", "can_operate", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := extractionTx(tx)
		if err != nil {
			return err
		}
		out.ID = r.ID
		return t.RetryExtractionJob(ctx, r.ID, r.ExpectedFence)
	})
	if err != nil {
		return model.JobStatus{}, err
	}
	return s.GetJob(ctx, c, model.JobQuery{ID: r.ID, Scope: r.Scope, Purpose: r.Purpose})
}
