package services

import (
	"bytes"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/model"
	"io"
	"math"
	"slices"
	"unicode/utf8"
)

func parseExtraction(body []byte, maximum int) (model.ExtractionOutput, error) {
	var out model.ExtractionOutput
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil {
		return out, model.ErrInvalid
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return out, model.ErrInvalid
	}
	if out.Candidates == nil || len(out.Candidates) > maximum {
		return out, model.ErrInvalid
	}
	return out, nil
}
func candidateIssue(c model.ExtractionCandidate, input model.ExtractionInput) string {
	if !slices.Contains(input.Subjects, c.Subject) {
		return "unbound_subject"
	}
	if c.Discriminator != "" && !model.ValidID(c.Discriminator) {
		return "invalid_discriminator"
	}
	var definition *model.PredicateVersion
	for i := range input.Predicates {
		p := &input.Predicates[i]
		if p.Predicate == c.Predicate && p.Version == c.SchemaVersion {
			definition = p
			break
		}
	}
	if definition == nil {
		return "unbound_predicate"
	}
	if c.Uncertainty != "asserted" {
		return "uncertain_or_nonaffirmative_assertion"
	}
	if definition.ValidateFact(c.Value, c.Valid) != nil {
		return "invalid_value"
	}
	if c.Valid.Validate() != nil {
		return "invalid_effective_interval"
	}
	if len(c.Rationale) > 2048 || math.IsNaN(c.Confidence) || math.IsInf(c.Confidence, 0) || c.Confidence < 0 || c.Confidence > 1 {
		return "invalid_diagnostics"
	}
	return extractionSourceIssue(c.Sources, input)
}

func extractionSourceIssue(sources []model.SourceSpan, input model.ExtractionInput) string {
	if len(sources) == 0 || len(sources) > 64 {
		return "missing_or_excessive_sources"
	}
	supported := false
	for _, span := range sources {
		var message *model.ExtractionInputMessage
		found := false
		for i := range input.Messages {
			m := &input.Messages[i]
			if m.EvidenceID != span.EvidenceID {
				continue
			}
			found = true
			if span.Start < m.SourceStart || span.End <= span.Start || span.End-m.SourceStart > len(m.Content) {
				continue
			}
			quote := m.Content[span.Start-m.SourceStart : span.End-m.SourceStart]
			if utf8.ValidString(quote) && quote == span.Quote {
				message = m
				break
			}
		}
		if !found {
			return "source_not_supplied"
		}
		if message == nil {
			return "invalid_source_span"
		}
		if message.SpeakerRole != "assistant" && message.SpeakerRole != "system" {
			supported = true
		}
	}
	if !supported {
		return "assistant_or_system_only_assertion"
	}
	return ""
}
