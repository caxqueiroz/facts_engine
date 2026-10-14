package services

import (
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestExtractionRangesPreserveOffsetsAndOmissions(t *testing.T) {
	original := []byte("私 like tea and coffee.")
	c := model.ConversationRecord{EvidenceIDs: []string{"one", "two"}, Request: model.ConversationRequest{Messages: []model.ConversationMessage{{SpeakerRole: "customer", Object: model.ObjectRef{Size: int64(len(original))}}, {Object: model.ObjectRef{Size: 5}}}}}
	selected := []model.ExtractionInputRange{{EvidenceID: "one", Start: 9, End: 12}, {EvidenceID: "one", Start: 17, End: 23}}
	ranges, excluded, size, err := planExtractionRanges(c, selected, "original-ranges-v1")
	if err != nil || size != 9 || len(excluded) != 4 || excluded[3].EvidenceID != "two" || excluded[3].End != 5 {
		t.Fatal(ranges, excluded, size, err)
	}
	input := model.ExtractionInput{}
	for _, r := range ranges[0] {
		m, err := extractionRangeMessage(c.Request.Messages[0], r, original)
		if err != nil {
			t.Fatal(err)
		}
		input.Messages = append(input.Messages, m)
	}
	for _, span := range []model.SourceSpan{{EvidenceID: "one", Start: 9, End: 12, Quote: "tea"}, {EvidenceID: "one", Start: 17, End: 23, Quote: "coffee"}} {
		if issue := extractionSourceIssue([]model.SourceSpan{span}, input); issue != "" {
			t.Fatal(issue, span)
		}
	}
	for _, span := range []model.SourceSpan{{EvidenceID: "one", Start: 0, End: 3, Quote: "tea"}, {EvidenceID: "one", Start: 9, End: 23, Quote: "tea and coffee"}, {EvidenceID: "two", Start: 0, End: 3, Quote: "tea"}} {
		if issue := extractionSourceIssue([]model.SourceSpan{span}, input); issue == "" {
			t.Fatal("accepted omitted or relative citation", span)
		}
	}
	for _, selected := range [][]model.ExtractionInputRange{
		{{EvidenceID: "unknown", End: 1}}, {{EvidenceID: "one", Start: -1, End: 1}}, {{EvidenceID: "one", Start: 1, End: 1}}, {{EvidenceID: "one", End: 100}},
		{{EvidenceID: "two", End: 1}, {EvidenceID: "one", End: 1}}, {{EvidenceID: "one", End: 10}, {EvidenceID: "one", Start: 9, End: 12}},
	} {
		if _, _, _, err := planExtractionRanges(c, selected, "original-ranges-v1"); err == nil {
			t.Fatal("accepted invalid selection", selected)
		}
	}
	if _, _, _, err := planExtractionRanges(c, selected, ""); err == nil {
		t.Fatal("unapproved selection")
	}
	if _, err := extractionRangeMessage(c.Request.Messages[0], model.ExtractionInputRange{EvidenceID: "one", Start: 1, End: 3}, original); err == nil {
		t.Fatal("split UTF-8 accepted")
	}
	ranges, excluded, size, err = planExtractionRanges(c, nil, "")
	if err != nil || len(excluded) != 0 || size != int64(len(original)+5) || len(ranges[1]) != 1 {
		t.Fatal("legacy whole conversation", ranges, excluded, size, err)
	}
}
