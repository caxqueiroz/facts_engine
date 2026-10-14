package services

import (
	"unicode/utf8"

	"github.com/caxqueiroz/facts_engine/model"
)

// Selection is explicit: never silently truncate or join across omitted text.
// Ranges must follow conversation order and cannot overlap within a message.
func planExtractionRanges(c model.ConversationRecord, selected []model.ExtractionInputRange, version string) ([][]model.ExtractionInputRange, []model.ExtractionInputRange, int64, error) {
	ranges := make([][]model.ExtractionInputRange, len(c.Request.Messages))
	if len(c.EvidenceIDs) != len(ranges) || len(selected) > 256 || (len(selected) > 0 && version != "original-ranges-v1") {
		return nil, nil, 0, model.ErrInvalid
	}
	indices := make(map[string]int, len(ranges))
	for i, evidenceID := range c.EvidenceIDs {
		indices[evidenceID] = i
	}
	previous, end := -1, 0
	for _, r := range selected {
		i, ok := indices[r.EvidenceID]
		if !ok || i < previous || (i == previous && r.Start < end) || r.Start < 0 || r.End <= r.Start || int64(r.End) > c.Request.Messages[i].Object.Size {
			return nil, nil, 0, model.ErrInvalid
		}
		ranges[i] = append(ranges[i], r)
		previous, end = i, r.End
	}
	var size int64
	var excluded []model.ExtractionInputRange
	for i, m := range c.Request.Messages {
		if len(selected) == 0 {
			ranges[i] = []model.ExtractionInputRange{{EvidenceID: c.EvidenceIDs[i], End: int(m.Object.Size)}}
		}
		start := 0
		for _, r := range ranges[i] {
			if start < r.Start {
				excluded = append(excluded, model.ExtractionInputRange{EvidenceID: c.EvidenceIDs[i], Start: start, End: r.Start})
			}
			size += int64(r.End - r.Start)
			start = r.End
		}
		if int64(start) < m.Object.Size {
			excluded = append(excluded, model.ExtractionInputRange{EvidenceID: c.EvidenceIDs[i], Start: start, End: int(m.Object.Size)})
		}
	}
	return ranges, excluded, size, nil
}

func extractionRangeMessage(m model.ConversationMessage, r model.ExtractionInputRange, original []byte) (model.ExtractionInputMessage, error) {
	if r.Start < 0 || r.End < r.Start || r.End > len(original) || int64(len(original)) != m.Object.Size || !utf8.Valid(original[r.Start:r.End]) {
		return model.ExtractionInputMessage{}, model.ErrInvalid
	}
	return model.ExtractionInputMessage{EvidenceID: r.EvidenceID, SourceStart: r.Start, Speaker: m.Speaker, SpeakerRole: m.SpeakerRole, SourceClass: m.SourceClass, SourceTime: m.SourceTime, Content: string(original[r.Start:r.End])}, nil
}
