package services

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"slices"
	"time"
	"unicode/utf8"
)

func validConversation(r model.ConversationRequest) bool {
	if !model.ValidID(r.Key) || !model.ValidID(r.Scope) || !model.ValidID(r.SourceSystem) || !model.ValidID(r.ConversationID) || !model.ValidID(r.Revision) || len(r.Messages) < 1 || len(r.Messages) > 128 || r.Timezone == "" || len(r.Timezone) > 128 {
		return false
	}
	if _, err := time.LoadLocation(r.Timezone); err != nil {
		return false
	}
	seen := map[string]bool{}
	var size int64
	for _, m := range r.Messages {
		if !model.ValidID(m.ID) || seen[m.ID] || !model.ValidID(m.Revision) || !model.ValidID(m.Speaker) || m.SourceTime.IsZero() || m.Classification == "" || len(m.Classification) > 256 || !model.ValidID(m.RetentionPolicy) || !slices.Contains([]string{"text/plain", "text/markdown"}, m.MediaType) || m.Object.Locator == "" || m.Object.Version == "" || !validDigest(m.Object.Digest) || m.Object.Size < 0 || m.Object.Size > 1<<20 {
			return false
		}
		seen[m.ID] = true
		size += m.Object.Size
		switch m.SpeakerRole {
		case "customer":
			if m.SourceClass != "customer" {
				return false
			}
		case "assistant":
			if m.SourceClass != "assistant" {
				return false
			}
		case "system":
			if m.SourceClass != "system" {
				return false
			}
		case "staff":
			if m.SourceClass != "staff" && m.SourceClass != "organization_record" {
				return false
			}
		default:
			return false
		}
	}
	return size <= 16<<20
}
func (s *Service) IngestConversation(ctx context.Context, c model.Caller, r model.ConversationRequest) (observationResult model.ConversationRecord, observationErr error) {
	ctx, observation := s.telemetry.Start(ctx, "ingest.conversation")
	defer observation.End(&observationErr)
	if !validConversation(r) {
		return model.ConversationRecord{}, model.ErrInvalid
	}
	r.Messages = append([]model.ConversationMessage(nil), r.Messages...)
	for i := range r.Messages {
		r.Messages[i].SourceTime = r.Messages[i].SourceTime.UTC()
	}
	d, err := s.authorize(ctx, c, r.Scope, "can_ingest", r.Command)
	if err != nil {
		return model.ConversationRecord{}, err
	}
	receipts := []model.AuthorizationDecision{d}
	d, err = s.authorize(ctx, c, r.Scope, "can_read_evidence_metadata", r.Command)
	if err != nil {
		return model.ConversationRecord{}, err
	}
	receipts = append(receipts, d)
	if slices.ContainsFunc(r.Messages, func(m model.ConversationMessage) bool { return m.SourceClass == "organization_record" }) {
		d, err = s.authorize(ctx, c, r.Scope, "can_ingest_organization_record", r.Command)
		if err != nil {
			return model.ConversationRecord{}, err
		}
		receipts = append(receipts, d)
	}
	record := model.ConversationRecord{ID: model.ConversationRecordID(c.Tenant, r), Tenant: c.Tenant, Digest: model.ConversationDigest(r), Caller: c, Request: r, Authorization: receipts}
	// Resolve committed source revisions before reading external storage again.
	var existing model.ConversationRecord
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, ok := tx.(model.ConversationTx)
		if !ok {
			return model.ErrPrecondition
		}
		var err error
		existing, err = t.Conversation(ctx, record.ID)
		return err
	})
	if err == nil && existing.Digest != record.Digest {
		return model.ConversationRecord{}, model.ErrConflict
	}
	if err != nil && !errors.Is(err, model.ErrNotFound) {
		return model.ConversationRecord{}, err
	}
	if errors.Is(err, model.ErrNotFound) {
		for _, m := range r.Messages {
			b, err := s.verify(ctx, c.Tenant, model.MessageEvidence(c.Tenant, r, m))
			if err != nil {
				return model.ConversationRecord{}, err
			}
			if !utf8.Valid(b) {
				return model.ConversationRecord{}, model.ErrInvalid
			}
		}
	}
	result, err := s.mutate(ctx, c, r.Scope, "ingest_conversation", "can_ingest", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, ok := tx.(model.ConversationTx)
		if !ok {
			return model.ErrPrecondition
		}
		old, err := t.Conversation(ctx, record.ID)
		if err == nil {
			if old.Digest != record.Digest {
				return model.ErrConflict
			}
			out.ID = old.ID
			out.ContentDigest = old.Digest
			return nil
		}
		if !errors.Is(err, model.ErrNotFound) {
			return err
		}
		for _, receipt := range receipts {
			if time.Since(receipt.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		// Reset on transaction retry. Every message/evidence link commits together.
		record.EvidenceIDs = nil
		for _, m := range r.Messages {
			id, err := tx.InsertEvidence(ctx, model.MessageEvidence(c.Tenant, r, m))
			if err != nil {
				return err
			}
			record.EvidenceIDs = append(record.EvidenceIDs, id)
		}
		if err = t.StageConversation(ctx, record); err != nil {
			return err
		}
		out.ID = record.ID
		out.ContentDigest = record.Digest
		return nil
	})
	if err != nil {
		return model.ConversationRecord{}, err
	}
	return s.GetConversation(ctx, c, model.ConversationQuery{ID: result.ID, Scope: r.Scope, Purpose: r.Purpose})
}
func (s *Service) GetConversation(ctx context.Context, c model.Caller, q model.ConversationQuery) (model.ConversationRecord, error) {
	if !model.ValidID(q.ID) {
		return model.ConversationRecord{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, q.Scope, "can_read_evidence_metadata", model.Command{Purpose: q.Purpose}); err != nil {
		return model.ConversationRecord{}, err
	}
	var r model.ConversationRecord
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, ok := tx.(model.ConversationTx)
		if !ok {
			return model.ErrPrecondition
		}
		var err error
		r, err = t.Conversation(ctx, q.ID)
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		return model.ConversationRecord{}, model.ErrDenied
	}
	if err != nil {
		return model.ConversationRecord{}, err
	}
	if r.Tenant != c.Tenant || r.Request.Scope != q.Scope {
		return model.ConversationRecord{}, model.ErrDenied
	}
	if model.ConversationDigest(r.Request) != r.Digest || len(r.EvidenceIDs) != len(r.Request.Messages) {
		return model.ConversationRecord{}, model.ErrPrecondition
	}
	if _, err = s.authorize(ctx, c, q.Scope, "can_read_evidence_metadata", model.Command{Purpose: q.Purpose}); err != nil {
		return model.ConversationRecord{}, err
	}
	return r, nil
}
