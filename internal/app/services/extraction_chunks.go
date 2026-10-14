package services

import (
	"context"
	"unicode/utf8"

	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) PlanExtractionChunks(ctx context.Context, c model.Caller, q model.ExtractionChunkQuery) (model.ExtractionChunkPlan, error) {
	empty := model.ExtractionChunkPlan{}
	if !model.ValidID(q.ConversationID) || !model.ValidID(q.ProfileID) || q.MaxSourceBytes < utf8.UTFMax {
		return empty, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, q.Scope, "can_extract", model.Command{Purpose: q.Purpose}); err != nil {
		return empty, err
	}
	approved, ok := s.extractors[q.ProfileID]
	if !ok || approved.Profile.InputSelectionVersion != "original-ranges-v1" || approved.Profile.ParserVersion == "memory-refresh-json-v1" || q.MaxSourceBytes > approved.Profile.MaxInputBytes {
		return empty, model.ErrPrecondition
	}
	conversation, err := s.GetConversation(ctx, c, model.ConversationQuery{ID: q.ConversationID, Scope: q.Scope, Purpose: q.Purpose})
	if err != nil {
		return empty, err
	}
	// Keep this check even when every selected source is empty.
	if _, err := s.authorize(ctx, c, q.Scope, "can_read_evidence", model.Command{Purpose: q.Purpose}); err != nil {
		return empty, err
	}
	ranges, excluded, _, err := planExtractionRanges(conversation, q.InputRanges, approved.Profile.InputSelectionVersion)
	if err != nil {
		return empty, err
	}
	plan := model.ExtractionChunkPlan{Version: "original-chunks-v1", Tenant: c.Tenant, Scope: q.Scope, ConversationID: conversation.ID, ConversationDigest: conversation.Digest, ProfileID: approved.Profile.ID, ProfileDigest: model.ExtractionProfileDigest(approved.Profile), MaxSourceBytes: q.MaxSourceBytes, Chunks: []model.ExtractionChunk{}, ExcludedRanges: excluded}
	for i, selected := range ranges {
		if len(selected) == 0 {
			continue
		}
		content, err := s.GetEvidenceContent(ctx, c, model.EvidenceQuery{ID: conversation.EvidenceIDs[i], Scope: q.Scope, Purpose: q.Purpose})
		if err != nil {
			return empty, err
		}
		for _, r := range selected {
			if _, err := extractionRangeMessage(conversation.Request.Messages[i], r, content.Bytes); err != nil {
				return empty, err
			}
			if err := appendExtractionChunks(&plan.Chunks, r, content.Bytes, q.MaxSourceBytes); err != nil {
				return empty, err
			}
		}
	}
	// Planning can involve multiple evidence reads. Recheck permissions before
	// returning source structure after a revocation during those reads.
	for _, permission := range []string{"can_extract", "can_read_evidence", "can_read_evidence_metadata"} {
		if _, err := s.authorize(ctx, c, q.Scope, permission, model.Command{Purpose: q.Purpose}); err != nil {
			return empty, err
		}
	}
	return plan, nil
}

// Bound both work and response size. A plan that exceeds the bound fails as a
// whole; callers can request a smaller source selection or larger byte budget.
const maximumExtractionChunks = 256

func appendExtractionChunks(chunks *[]model.ExtractionChunk, r model.ExtractionInputRange, original []byte, maximum int) error {
	if maximum < utf8.UTFMax || r.Start < 0 || r.End < r.Start || r.End > len(original) || !utf8.Valid(original[r.Start:r.End]) {
		return model.ErrInvalid
	}
	for start := r.Start; start < r.End; {
		if len(*chunks) == 0 || (*chunks)[len(*chunks)-1].SourceBytes == maximum || len((*chunks)[len(*chunks)-1].InputRanges) == 256 {
			if len(*chunks) >= maximumExtractionChunks {
				return model.ErrPrecondition
			}
			*chunks = append(*chunks, model.ExtractionChunk{})
		}
		chunk := &(*chunks)[len(*chunks)-1]
		end := start + min(maximum-chunk.SourceBytes, r.End-start)
		for end < r.End && end > start && !utf8.RuneStart(original[end]) {
			end--
		}
		if end == start {
			if len(*chunks) >= maximumExtractionChunks {
				return model.ErrPrecondition
			}
			*chunks = append(*chunks, model.ExtractionChunk{})
			continue
		}
		chunk.InputRanges = append(chunk.InputRanges, model.ExtractionInputRange{EvidenceID: r.EvidenceID, Start: start, End: end})
		chunk.SourceBytes += end - start
		start = end
	}
	return nil
}
