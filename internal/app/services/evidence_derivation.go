package services

import (
	"bytes"
	"context"
	"unicode/utf8"

	"github.com/caxqueiroz/facts_engine/model"
)

func validateRedaction(original, derived []byte, ranges []model.EvidenceCopyRange) error {
	if len(ranges) > 256 || !utf8.Valid(original) || !utf8.Valid(derived) {
		return model.ErrInvalid
	}
	previous, offset := 0, 0
	for _, r := range ranges {
		if r.OriginalStart < previous || r.OriginalEnd <= r.OriginalStart || r.OriginalEnd > len(original) || r.DerivedStart != offset {
			return model.ErrInvalid
		}
		source := original[r.OriginalStart:r.OriginalEnd]
		if !utf8.Valid(source) || len(source) > len(derived)-offset || !bytes.Equal(source, derived[offset:offset+len(source)]) {
			return model.ErrInvalid
		}
		previous = r.OriginalEnd
		offset += len(source)
	}
	if offset != len(derived) {
		return model.ErrInvalid
	}
	return nil
}

func (s *Service) RegisterRedactedEvidence(ctx context.Context, c model.Caller, r model.RedactedEvidenceRequest) (model.CommitResult, error) {
	if !model.ValidID(r.ID) || !model.ValidID(r.ParentEvidenceID) || r.ID == r.ParentEvidenceID || !model.ValidID(r.PolicyVersion) || r.Object.Locator == "" || r.Object.Version == "" || r.Object.Size < 0 || r.Object.Size > 16<<20 {
		return model.CommitResult{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, r.Scope, "can_propose", r.Command); err != nil {
		return model.CommitResult{}, err
	}
	if !s.redactionPolicies[r.PolicyVersion] {
		return model.CommitResult{}, model.ErrPrecondition
	}
	parent, err := s.GetEvidenceContent(ctx, c, model.EvidenceQuery{ID: r.ParentEvidenceID, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil {
		return model.CommitResult{}, err
	}
	if parent.Metadata.Derivation != nil || (parent.Metadata.MediaType != "text/plain" && parent.Metadata.MediaType != "text/markdown") {
		return model.CommitResult{}, model.ErrPrecondition
	}
	e := parent.Metadata
	e.ID = r.ID
	e.Object = r.Object
	e.Derivation = &model.EvidenceDerivation{ParentEvidenceID: r.ParentEvidenceID, TransformationVersion: "utf8-omit-v1", PolicyVersion: r.PolicyVersion, Ranges: append([]model.EvidenceCopyRange(nil), r.Ranges...)}
	e.SourceObject = model.Digest([]string{"redacted-evidence-v1", parent.Metadata.ID, r.PolicyVersion, model.Digest(e.Derivation)})
	e.SourceRevision = "v1"
	derived, err := s.verify(ctx, c.Tenant, e)
	if err != nil {
		return model.CommitResult{}, err
	}
	if err = validateRedaction(parent.Bytes, derived, r.Ranges); err != nil {
		return model.CommitResult{}, err
	}
	recheck := func() error { _, err := s.authorize(ctx, c, r.Scope, "can_read_evidence", r.Command); return err }
	return s.mutate(ctx, c, r.Scope, "register_redacted_evidence", "can_propose", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		evidenceID, err := tx.InsertEvidence(ctx, e)
		out.ID = evidenceID
		out.ContentDigest = e.Object.Digest
		return err
	}, freshnessCheck(recheck))
}

func validateDerivativeSources(ctx context.Context, tx model.Tx, sources []model.SourceSpan) error {
	for _, span := range sources {
		evidence, err := tx.Evidence(ctx, span.EvidenceID)
		if err != nil {
			return err
		}
		if evidence.Derivation != nil {
			if _, err := evidence.Derivation.OriginalSpan(span); err != nil {
				return err
			}
		}
	}
	return nil
}
