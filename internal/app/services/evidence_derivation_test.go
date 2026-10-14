package services

import (
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
)

func TestRedactedEvidenceRequiresExactMappedUTF8(t *testing.T) {
	original := []byte("私 SECRET tea")
	ranges := []model.EvidenceCopyRange{{OriginalEnd: 4}, {OriginalStart: 11, OriginalEnd: 14, DerivedStart: 4}}
	if err := validateRedaction(original, []byte("私 tea"), ranges); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		body   string
		ranges []model.EvidenceCopyRange
	}{
		{"私 xxx", ranges}, {"私 tea!", ranges}, {"私 tea", []model.EvidenceCopyRange{{OriginalEnd: 4}, {OriginalStart: 11, OriginalEnd: 14, DerivedStart: 3}}},
		{"私", []model.EvidenceCopyRange{{OriginalStart: 1, OriginalEnd: 3}}}, {"私私", []model.EvidenceCopyRange{{OriginalEnd: 3}, {OriginalEnd: 3, DerivedStart: 3}}},
	} {
		if err := validateRedaction(original, []byte(test.body), test.ranges); err == nil {
			t.Fatal("invalid mapping accepted", test)
		}
	}
	if err := validateRedaction(original, nil, nil); err != nil {
		t.Fatal("fully redacted", err)
	}
	derivation := model.EvidenceDerivation{ParentEvidenceID: "parent", TransformationVersion: "utf8-omit-v1", Ranges: ranges}
	mapped, err := derivation.OriginalSpan(model.SourceSpan{Start: 4, End: 7, Quote: "tea"})
	if err != nil || mapped.EvidenceID != "parent" || mapped.Start != 11 || mapped.End != 14 || mapped.Quote != "tea" {
		t.Fatal(mapped, err)
	}
	if _, err := derivation.OriginalSpan(model.SourceSpan{Start: 0, End: 7, Quote: "私 tea"}); err == nil {
		t.Fatal("citation bridged omitted content")
	}
}
