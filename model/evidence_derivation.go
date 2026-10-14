package model

// EvidenceCopyRange maps an unchanged derivative segment to original bytes.
type EvidenceCopyRange struct {
	OriginalStart, OriginalEnd, DerivedStart int
}

// EvidenceDerivation is server-validated omission provenance. Classification,
// source authority and retention policy are inherited from the original.
type EvidenceDerivation struct {
	ParentEvidenceID, TransformationVersion, PolicyVersion string
	Ranges                                                 []EvidenceCopyRange
}

type RedactedEvidenceRequest struct {
	Command
	ID, Scope, ParentEvidenceID, PolicyVersion string
	Object                                     ObjectRef
	Ranges                                     []EvidenceCopyRange
}

// OriginalSpan translates a citation only when it fits within one unchanged
// segment. It never joins text across an omission or cites inserted text.
func (d EvidenceDerivation) OriginalSpan(span SourceSpan) (SourceSpan, error) {
	if d.TransformationVersion != "utf8-omit-v1" || span.Start < 0 || span.End <= span.Start {
		return SourceSpan{}, ErrInvalid
	}
	for _, r := range d.Ranges {
		if span.Start >= r.DerivedStart && span.End-r.DerivedStart <= r.OriginalEnd-r.OriginalStart {
			return SourceSpan{EvidenceID: d.ParentEvidenceID, Start: r.OriginalStart + span.Start - r.DerivedStart, End: r.OriginalStart + span.End - r.DerivedStart, Quote: span.Quote}, nil
		}
	}
	return SourceSpan{}, ErrInvalid
}
