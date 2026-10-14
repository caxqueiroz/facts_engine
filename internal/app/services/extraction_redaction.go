package services

import (
	"context"
	"unicode/utf8"

	"github.com/caxqueiroz/facts_engine/model"
)

// Resolve only approved immutable derivatives. The output retains original IDs
// and offsets so providers cannot cite concealed bytes or switch source authority.
func (s *Service) prepareExtractionMessages(ctx context.Context, c model.Caller, conversation model.ConversationRecord, r model.ExtractionRequest, p model.ExtractionProfile) ([]model.ExtractionInputMessage, []model.ExtractionInputRange, int64, error) {
	fail := func(err error) ([]model.ExtractionInputMessage, []model.ExtractionInputRange, int64, error) {
		return nil, nil, 0, err
	}
	ranges, _, _, err := planExtractionRanges(conversation, r.InputRanges, p.InputSelectionVersion)
	if err != nil {
		return fail(err)
	}
	if len(r.RedactedEvidenceIDs) > 128 || (p.RedactionPolicyVersion == "" && len(r.RedactedEvidenceIDs) > 0) {
		return fail(model.ErrInvalid)
	}
	if p.RedactionPolicyVersion != "" {
		if !s.redactionPolicies[p.RedactionPolicyVersion] {
			return fail(model.ErrPrecondition)
		}
		// Bound dependency payloads contain values and source quotes, not just IDs.
		// A conversation redaction policy cannot authorize disclosing those payloads.
		for _, target := range r.MemoryTargets {
			if len(target.Dependencies) > 0 {
				return fail(model.ErrPrecondition)
			}
		}
	}
	originals := map[string]bool{}
	for _, id := range conversation.EvidenceIDs {
		originals[id] = true
	}
	derivatives := map[string]model.EvidenceContent{}
	for _, id := range r.RedactedEvidenceIDs {
		content, err := s.GetEvidenceContent(ctx, c, model.EvidenceQuery{ID: id, Scope: r.Scope, Purpose: r.Purpose})
		if err != nil {
			return fail(err)
		}
		d := content.Metadata.Derivation
		if d == nil || d.TransformationVersion != "utf8-omit-v1" || d.PolicyVersion != p.RedactionPolicyVersion || !originals[d.ParentEvidenceID] {
			return fail(model.ErrPrecondition)
		}
		if _, exists := derivatives[d.ParentEvidenceID]; exists {
			return fail(model.ErrInvalid)
		}
		derivatives[d.ParentEvidenceID] = content
	}
	messages := []model.ExtractionInputMessage{}
	var excluded []model.ExtractionInputRange
	var size int64
	for i, m := range conversation.Request.Messages {
		evidenceID := conversation.EvidenceIDs[i]
		included := []model.ExtractionInputRange{}
		if len(ranges[i]) > 0 {
			derivative, found := derivatives[evidenceID]
			if p.RedactionPolicyVersion != "" && !found {
				return fail(model.ErrPrecondition)
			}
			original, err := s.GetEvidenceContent(ctx, c, model.EvidenceQuery{ID: evidenceID, Scope: r.Scope, Purpose: r.Purpose})
			if err != nil {
				return fail(err)
			}
			if found {
				if err = validateRedaction(original.Bytes, derivative.Bytes, derivative.Metadata.Derivation.Ranges); err != nil {
					return fail(model.ErrPrecondition)
				}
			}
			for _, selected := range ranges[i] {
				// Check the user's selection even if it lies entirely in concealed bytes.
				message, err := extractionRangeMessage(m, selected, original.Bytes)
				if err != nil {
					return fail(err)
				}
				if !found {
					messages = append(messages, message)
					included = append(included, selected)
					size += int64(len(message.Content))
					continue
				}
				for _, copyRange := range derivative.Metadata.Derivation.Ranges {
					start, end := max(selected.Start, copyRange.OriginalStart), min(selected.End, copyRange.OriginalEnd)
					if start >= end {
						continue
					}
					offset := copyRange.DerivedStart + start - copyRange.OriginalStart
					text := derivative.Bytes[offset : offset+end-start]
					if !utf8.Valid(text) {
						return fail(model.ErrPrecondition)
					}
					message.SourceStart = start
					message.Content = string(text)
					message.DerivativeEvidenceID = derivative.Metadata.ID
					message.DerivativeDigest = derivative.Metadata.Object.Digest
					message.DerivativeStart = offset
					messages = append(messages, message)
					included = append(included, model.ExtractionInputRange{EvidenceID: evidenceID, Start: start, End: end})
					size += int64(len(text))
					if len(messages) > 256 {
						return fail(model.ErrPrecondition)
					}
				}
			}
		}
		start := 0
		for _, part := range included {
			if start < part.Start {
				excluded = append(excluded, model.ExtractionInputRange{EvidenceID: evidenceID, Start: start, End: part.Start})
			}
			start = part.End
		}
		if int64(start) < m.Object.Size {
			excluded = append(excluded, model.ExtractionInputRange{EvidenceID: evidenceID, Start: start, End: int(m.Object.Size)})
		}
	}
	return messages, excluded, size, nil
}
