package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"unicode/utf8"

	"github.com/caxqueiroz/facts_engine/model"
)

func validateMemoryExtractionTargets(targets []model.MemoryExtractionTarget) error {
	if len(targets) == 0 || len(targets) > 64 {
		return model.ErrInvalid
	}
	seen := map[string]bool{}
	allVersions := map[string]bool{}
	for _, target := range targets {
		if !model.ValidID(target.ID) || !model.ValidID(target.Subtype) || !model.ValidID(target.SchemaVersion) || seen[target.ID] || len(target.Dependencies) > 64 {
			return model.ErrInvalid
		}
		seen[target.ID] = true
		versions := map[string]bool{}
		for _, d := range target.Dependencies {
			if !model.ValidID(d.Scope) || !model.ValidID(d.RecordID) || !model.ValidID(d.VersionID) || !validDigest(d.ContentDigest) || !slices.Contains([]string{"assertion", "uncertain", "disputed"}, d.Label) || versions[d.VersionID] {
				return model.ErrInvalid
			}
			versions[d.VersionID] = true
			allVersions[d.VersionID] = true
		}
	}
	if len(allVersions) > 64 {
		return model.ErrInvalid
	}
	return nil
}
func memoryExtractionScopes(r model.ExtractionRequest) []string {
	scopes := []string{r.Scope}
	for _, target := range r.MemoryTargets {
		for _, d := range target.Dependencies {
			scopes = append(scopes, d.Scope)
		}
	}
	slices.Sort(scopes)
	return slices.Compact(scopes)
}
func (s *Service) bindMemoryExtraction(ctx context.Context, tx model.Tx, j *model.ExtractionJob) error {
	t, err := memoryTx(tx)
	if err != nil {
		return err
	}
	j.MemoryTypes = nil
	j.DependencyVersions = nil
	types := map[string]bool{}
	versions := map[string]bool{}
	for _, target := range j.Request.MemoryTargets {
		active, err := t.ActiveMemoryType(ctx, j.Request.Scope, target.Subtype)
		if err != nil {
			return err
		}
		if active != target.SchemaVersion {
			return model.ErrPrecondition
		}
		definition, err := t.MemoryType(ctx, j.Request.Scope, target.Subtype, target.SchemaVersion)
		if err != nil {
			return err
		}
		key := model.Digest([]string{target.Subtype, target.SchemaVersion})
		if !types[key] {
			types[key] = true
			j.MemoryTypes = append(j.MemoryTypes, definition)
		}
		for _, d := range target.Dependencies {
			v, err := tx.Version(ctx, d.RecordID, d.VersionID)
			if err != nil {
				return err
			}
			if v.Digest != d.ContentDigest || v.Proposal.Key.Scope != d.Scope {
				return model.ErrPrecondition
			}
			if !versions[v.ID] {
				versions[v.ID] = true
				j.DependencyVersions = append(j.DependencyVersions, v)
			}
		}
		encoded, err := json.Marshal(j)
		if err != nil {
			return err
		}
		if len(encoded) > j.Profile.MaxInputBytes {
			return model.ErrPrecondition
		}
	}
	return nil
}
func parseMemoryExtraction(body []byte, maximum int) (model.MemoryExtractionOutput, error) {
	var output model.MemoryExtractionOutput
	if !utf8.Valid(body) {
		return output, model.ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&output); err != nil {
		return output, model.ErrInvalid
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return output, model.ErrInvalid
	}
	if output.Candidates == nil || len(output.Candidates) > maximum {
		return output, model.ErrInvalid
	}
	return output, nil
}
func memoryCandidateIssue(c model.MemoryExtractionCandidate, input model.ExtractionInput) string {
	bound := false
	for _, target := range input.MemoryTargets {
		if target.ID == c.ID && target.Subtype == c.Subtype && target.SchemaVersion == c.SchemaVersion {
			bound = true
		}
	}
	if !bound {
		return "unbound_memory_target"
	}
	if len(c.Body) == 0 || len(c.Body) > 65536 || !utf8.ValidString(c.Body) {
		return "invalid_memory_body"
	}
	if c.Valid.From == nil || c.Valid.Validate() != nil {
		return "invalid_effective_interval"
	}
	if c.Uncertainty != "asserted" {
		return "uncertain_or_nonaffirmative_assertion"
	}
	if len(c.Rationale) > 2048 || math.IsNaN(c.Confidence) || math.IsInf(c.Confidence, 0) || c.Confidence < 0 || c.Confidence > 1 {
		return "invalid_diagnostics"
	}
	return extractionSourceIssue(c.Sources, input)
}
func (s *Service) appendExtractedMemories(ctx context.Context, tx model.ExtractionRunTx, c model.Caller, job model.ExtractionJob, input model.ExtractionInput, output model.MemoryExtractionOutput, attempt string, auth []model.AuthorizationDecision, result *model.ExtractionResult) (observationErr error) {
	ctx, observation := s.telemetry.Start(ctx, "validate.memory_proposals")
	defer observation.End(&observationErr)
	t, ok := tx.(model.MemoryExtractionTx)
	if !ok {
		return model.ErrPrecondition
	}
	counts := map[string]int{}
	for _, candidate := range output.Candidates {
		counts[candidate.ID]++
	}
	order := make([]int, len(output.Candidates))
	for i := range order {
		order[i] = i
	}
	key := func(candidate model.MemoryExtractionCandidate) model.MemoryKey {
		return model.MemoryKey{Scope: job.Request.Scope, ID: candidate.ID}
	}
	sort.Slice(order, func(i, j int) bool {
		return model.MemoryRecordID(key(output.Candidates[order[i]])) < model.MemoryRecordID(key(output.Candidates[order[j]]))
	})
	records := map[string]model.Record{}
	for _, ordinal := range order {
		candidate := output.Candidates[ordinal]
		code := memoryCandidateIssue(candidate, input)
		if counts[candidate.ID] > 1 {
			code = "multiple_candidates_for_key"
		}
		if code != "" {
			result.Issues = append(result.Issues, model.ExtractionIssue{Candidate: ordinal, Code: code})
			continue
		}
		active, err := t.ActiveMemoryType(ctx, job.Request.Scope, candidate.Subtype)
		if err != nil {
			return err
		}
		if active != candidate.SchemaVersion {
			result.Issues = append(result.Issues, model.ExtractionIssue{Candidate: ordinal, Code: "schema_changed"})
			continue
		}
		definition, err := t.MemoryType(ctx, job.Request.Scope, candidate.Subtype, candidate.SchemaVersion)
		if err != nil {
			return err
		}
		policy, err := t.Policy(ctx, job.Request.Scope, definition.PolicyID)
		if err != nil {
			return err
		}
		allowed := true
		for _, span := range candidate.Sources {
			evidence, err := tx.Evidence(ctx, span.EvidenceID)
			if err != nil {
				return err
			}
			if !slices.Contains(policy.SourceClasses, evidence.SourceClass) {
				allowed = false
			}
		}
		if !allowed {
			result.Issues = append(result.Issues, model.ExtractionIssue{Candidate: ordinal, Code: "source_authority_not_allowed"})
			continue
		}
		record, err := t.LockMemory(ctx, key(candidate))
		if err != nil {
			return err
		}
		records[candidate.ID] = record
	}
	var proposeAuth model.AuthorizationDecision
	for _, d := range auth {
		if d.Request.Caller == c && d.Request.Permission == "can_propose" {
			proposeAuth = d
			break
		}
	}
	if !proposeAuth.Allowed {
		return model.ErrDenied
	}
	revision, err := tx.Revision(ctx)
	if err != nil {
		return err
	}
	for _, ordinal := range order {
		candidate := output.Candidates[ordinal]
		record, ok := records[candidate.ID]
		if !ok {
			continue
		}
		var dependencies []model.MemoryDependency
		for _, target := range input.MemoryTargets {
			if target.ID == candidate.ID {
				dependencies = target.Dependencies
			}
		}
		request := model.MemoryProposal{Command: model.Command{Key: fmt.Sprintf("%s-memory-%d", attempt, ordinal), Purpose: job.Request.Purpose, CorrelationID: job.Request.CorrelationID}, Key: key(candidate), Subtype: candidate.Subtype, SchemaVersion: candidate.SchemaVersion, Body: candidate.Body, Valid: candidate.Valid, Sources: candidate.Sources, Dependencies: dependencies, ExpectedRevision: record.Revision}
		versions, err := t.MemoryVersions(ctx, record.ID, revision)
		if err != nil {
			return err
		}
		timeline, err := t.MemoryTimeline(ctx, record.ID, revision)
		if err != nil {
			return err
		}
		digest := model.MemoryDigest(request)
		duplicate := false
		for _, v := range versions {
			if v.Digest == digest {
				duplicate = true
				result.Relations = append(result.Relations, model.ExtractionRelation{Candidate: ordinal, RecordID: record.ID, VersionID: v.ID, Kind: "duplicate"})
			}
			for _, entry := range timeline {
				if entry.VersionID == v.ID && entry.State != "retracted" && entry.Valid.Overlaps(request.Valid) && v.Digest != digest {
					result.Relations = append(result.Relations, model.ExtractionRelation{Candidate: ordinal, RecordID: record.ID, VersionID: v.ID, Kind: "overlapping_different_content"})
				}
			}
		}
		if duplicate {
			continue
		}
		var proposed model.CommitResult
		if err = s.stageMemoryProposal(ctx, tx, c, request, auth, &proposed); err != nil {
			return err
		}
		if _, err = tx.Append(ctx, model.Event{ID: id(), EntityID: proposed.ID, Type: "propose_memory", RecordKind: "memory", Scope: job.Request.Scope, RecordID: proposed.RecordID, VersionID: proposed.VersionID, StreamRevision: proposed.StreamRevision, Caller: c, Command: request.Command, Authorization: proposeAuth}); err != nil {
			return err
		}
		result.Proposals = append(result.Proposals, model.ExtractedProposal{Candidate: ordinal, RecordKind: "memory", RecordID: proposed.RecordID, VersionID: proposed.VersionID})
	}
	sort.Slice(result.Issues, func(i, j int) bool { return result.Issues[i].Candidate < result.Issues[j].Candidate })
	sort.Slice(result.Proposals, func(i, j int) bool { return result.Proposals[i].Candidate < result.Proposals[j].Candidate })
	return nil
}
