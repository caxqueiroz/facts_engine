package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/caxqueiroz/facts_engine/model"
)

var errRefreshInputLimit = errors.New("memory refresh input limit")

func (s *Service) StartMemoryRefresh(ctx context.Context, c model.Caller, r model.MemoryRefreshRequest) (model.JobStatus, error) {
	if !model.ValidID(r.RecordID) || !model.ValidID(r.VersionID) || !model.ValidID(r.ProfileID) {
		return model.JobStatus{}, model.ErrInvalid
	}
	approved, ok := s.extractors[r.ProfileID]
	if !ok || approved.Profile.ParserVersion != "memory-refresh-json-v1" {
		return model.JobStatus{}, model.ErrPrecondition
	}
	original, err := s.GetMemoryVersion(ctx, c, model.MemoryVersionQuery{Scope: r.Scope, RecordID: r.RecordID, VersionID: r.VersionID, Purpose: r.Purpose})
	if err != nil {
		return model.JobStatus{}, err
	}
	if len(original.Proposal.Dependencies) == 0 {
		return model.JobStatus{}, model.ErrRefreshNotNeeded
	}
	if r.Trigger != nil {
		event, err := s.memoryRefreshEvent(ctx, c, *r.Trigger, r.Purpose)
		if err != nil {
			return model.JobStatus{}, err
		}
		relevant := false
		for _, dependency := range original.Proposal.Dependencies {
			if dependency.RecordID == event.RecordID && dependency.Scope == event.Scope {
				relevant = true
			}
		}
		if !relevant || original.LedgerRevision > event.LedgerRevision {
			return model.JobStatus{}, model.ErrInvalid
		}
	}
	scopes := []string{r.Scope}
	for _, d := range original.Proposal.Dependencies {
		scopes = append(scopes, d.Scope)
	}
	slices.Sort(scopes)
	scopes = slices.Compact(scopes)
	job := model.ExtractionJob{ID: id(), Tenant: c.Tenant, Caller: c, Request: model.ExtractionRequest{Command: r.Command, Scope: r.Scope, ProfileID: r.ProfileID, RefreshTrigger: r.Trigger, Refresh: &model.VersionReference{RecordID: r.RecordID, VersionID: r.VersionID}}, Profile: approved.Profile, ProfileDigest: model.ExtractionProfileDigest(approved.Profile), RefreshScopes: scopes}
	job.Authorization, err = s.extractionAuthorizations(ctx, c, job)
	if err != nil {
		return model.JobStatus{}, err
	}
	result, err := s.mutate(ctx, c, r.Scope, "start_memory_refresh", "can_extract", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := memoryLifecycleTx(tx)
		if err != nil {
			return err
		}
		revision, at, err := selection(ctx, tx, 0)
		if err != nil {
			return err
		}
		accepted, err := t.MemoryTimeline(ctx, r.RecordID, revision)
		if err != nil {
			return err
		}
		active := false
		for _, entry := range accepted {
			if entry.VersionID == r.VersionID && entry.State == "accepted" {
				active = true
			}
		}
		if !active {
			return model.ErrRefreshNotNeeded
		}
		eligible, err := s.memoryDependencies(ctx, t, original, revision, at, false)
		if err != nil {
			return err
		}
		if eligible {
			return model.ErrRefreshNotNeeded
		}
		et, err := extractionTx(tx)
		if err != nil {
			return err
		}
		for _, d := range job.Authorization {
			if time.Since(d.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		if err = et.StageExtractionJob(ctx, job); err != nil {
			return err
		}
		out.ID = job.ID
		out.ContentDigest = job.ProfileDigest
		return nil
	})
	if err != nil {
		return model.JobStatus{}, err
	}
	return s.GetJob(ctx, c, model.JobQuery{ID: result.ID, Scope: r.Scope, Purpose: r.Purpose})
}

func (s *Service) memoryRefreshAuthorization(ctx context.Context, c model.Caller, j model.ExtractionJob) ([]model.AuthorizationDecision, error) {
	receipts := []model.AuthorizationDecision{}
	for _, scope := range j.RefreshScopes {
		for _, permission := range []string{"can_read", "can_read_history", "can_read_evidence"} {
			d, err := s.authorize(ctx, c, scope, permission, j.Request.Command)
			if err != nil {
				return nil, err
			}
			receipts = append(receipts, d)
		}
	}
	return receipts, nil
}

func (s *Service) buildMemoryRefresh(ctx context.Context, c model.Caller, j model.ExtractionJob) (*model.MemoryRefreshInput, error) {
	original, err := s.GetMemoryVersion(ctx, c, model.MemoryVersionQuery{Scope: j.Request.Scope, RecordID: j.Request.Refresh.RecordID, VersionID: j.Request.Refresh.VersionID, Purpose: j.Request.Purpose})
	if err != nil {
		return nil, err
	}
	input := &model.MemoryRefreshInput{Schema: "memory-refresh-input-v1", Original: original}
	err = s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		input.Dependencies = nil
		t, err := memoryLifecycleTx(tx)
		if err != nil {
			return err
		}
		revision, _, err := selection(ctx, tx, 0)
		if err != nil {
			return err
		}
		input.LedgerRevision = revision
		record, err := tx.Record(ctx, original.RecordID)
		if err != nil {
			return err
		}
		input.StreamRevision = record.Revision
		input.Definition, err = t.MemoryType(ctx, j.Request.Scope, original.Proposal.Subtype, original.Proposal.SchemaVersion)
		if err != nil {
			return err
		}
		for _, d := range original.Proposal.Dependencies {
			entries, err := t.Acceptances(ctx, d.RecordID, revision)
			if err != nil {
				return err
			}
			dependency := model.MemoryRefreshDependency{Scope: d.Scope, RecordID: d.RecordID}
			seen := map[string]bool{}
			for _, entry := range entries {
				if !entry.Valid.Overlaps(original.Proposal.Valid) {
					continue
				}
				dependency.Timeline = append(dependency.Timeline, entry)
				if seen[entry.VersionID] {
					continue
				}
				seen[entry.VersionID] = true
				v, err := tx.Version(ctx, d.RecordID, entry.VersionID)
				if err != nil {
					return err
				}
				if v.Proposal.Key.Scope != d.Scope {
					return model.ErrPrecondition
				}
				dependency.Versions = append(dependency.Versions, v)
			}
			input.Dependencies = append(input.Dependencies, dependency)
			encoded, err := json.Marshal(input)
			if err != nil {
				return err
			}
			if len(encoded) > j.Profile.MaxInputBytes {
				return errRefreshInputLimit
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var evidenceSize int64
	for _, span := range original.Proposal.Sources {
		metadata, err := s.GetEvidenceMetadata(ctx, c, model.EvidenceQuery{ID: span.EvidenceID, Scope: j.Request.Scope, Purpose: j.Request.Purpose})
		if err != nil {
			return nil, err
		}
		evidenceSize += metadata.Object.Size
		if evidenceSize > int64(j.Profile.MaxInputBytes) {
			return nil, errRefreshInputLimit
		}
	}
	for _, span := range original.Proposal.Sources {
		e, err := s.GetEvidenceContent(ctx, c, model.EvidenceQuery{ID: span.EvidenceID, Scope: j.Request.Scope, Purpose: j.Request.Purpose})
		if err != nil {
			return nil, err
		}
		input.Evidence = append(input.Evidence, model.ExtractionInputMessage{EvidenceID: span.EvidenceID, Content: string(e.Bytes)})
	}
	// Fact payloads can contain source quotes. Recheck dependency evidence access
	// before the retained request is exposed to the approved provider.
	if _, err = s.memoryRefreshAuthorization(ctx, c, j); err != nil {
		return nil, err
	}
	return input, nil
}

func parseMemoryRefresh(body []byte) (model.MemoryRefreshOutput, error) {
	var out model.MemoryRefreshOutput
	if !utf8.Valid(body) {
		return out, model.ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&out); err != nil {
		return out, model.ErrInvalid
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return out, model.ErrInvalid
	}
	if len(out.DeclineReason) > 2048 || !utf8.ValidString(out.DeclineReason) {
		return out, model.ErrInvalid
	}
	if out.DeclineReason != "" {
		if out.Body != "" || len(out.Dependencies) > 0 {
			return out, model.ErrInvalid
		}
		return out, nil
	}
	if len(out.Body) == 0 || len(out.Body) > 65536 || !utf8.ValidString(out.Body) || len(out.Dependencies) == 0 || len(out.Dependencies) > 64 {
		return out, model.ErrInvalid
	}
	return out, nil
}

func (s *Service) appendRefreshedMemory(ctx context.Context, tx model.ExtractionRunTx, c model.Caller, job model.ExtractionJob, input model.MemoryRefreshInput, output model.MemoryRefreshOutput, attempt string, auth []model.AuthorizationDecision, result *model.ExtractionResult) error {
	issue := func(code string) error {
		result.Issues = append(result.Issues, model.ExtractionIssue{Candidate: 0, Code: code})
		return nil
	}
	if output.DeclineReason != "" {
		return issue("refresh_declined")
	}
	// The provider may select only retained versions of the original dependency
	// records. It cannot silently remove dependencies or invent new provenance.
	if code := refreshDependencyIssue(input, output); code != "" {
		return issue(code)
	}
	t, err := memoryLifecycleTx(tx)
	if err != nil {
		return err
	}
	record, err := t.LockMemory(ctx, input.Original.Proposal.Key)
	if err != nil {
		return err
	}
	if record.Revision != input.StreamRevision {
		return issue("memory_changed")
	}
	request := input.Original.Proposal
	request.Command = model.Command{Key: attempt + "-memory", Purpose: job.Request.Purpose, CorrelationID: job.Request.CorrelationID}
	request.Body = output.Body
	request.Dependencies = output.Dependencies
	request.ExpectedRevision = record.Revision
	revision, at, err := selection(ctx, tx, 0)
	if err != nil {
		return err
	}
	timeline, err := t.MemoryTimeline(ctx, input.Original.RecordID, revision)
	if err != nil {
		return err
	}
	activeOriginal := false
	for _, entry := range timeline {
		if entry.VersionID == input.Original.ID && entry.State == "accepted" {
			activeOriginal = true
		}
	}
	if !activeOriginal {
		return issue("original_memory_retired")
	}
	restored, err := s.memoryDependencies(ctx, t, input.Original, revision, at, false)
	if err != nil {
		return err
	}
	if restored {
		return issue("dependency_restored")
	}
	eligible, err := s.memoryDependencies(ctx, t, model.MemoryVersion{Proposal: request}, revision, at, false)
	if err != nil {
		return err
	}
	if !eligible {
		return issue("dependency_ineligible")
	}
	active, err := t.ActiveMemoryType(ctx, request.Key.Scope, request.Subtype)
	if err != nil {
		return err
	}
	if active != request.SchemaVersion {
		return issue("schema_changed")
	}
	if model.MemoryDigest(request) == input.Original.Digest {
		return issue("unchanged_memory")
	}
	var proposeAuth model.AuthorizationDecision
	for _, d := range auth {
		if d.Request.Caller == c && d.Request.Permission == "can_propose" {
			proposeAuth = d
		}
	}
	if !proposeAuth.Allowed {
		return model.ErrDenied
	}
	var proposed model.CommitResult
	if err = s.stageMemoryProposal(ctx, tx, c, request, auth, &proposed); err != nil {
		return err
	}
	if _, err = tx.Append(ctx, model.Event{ID: id(), EntityID: proposed.ID, Type: "propose_memory", RecordKind: "memory", Scope: job.Request.Scope, RecordID: proposed.RecordID, VersionID: proposed.VersionID, StreamRevision: proposed.StreamRevision, Caller: c, Command: request.Command, Authorization: proposeAuth}); err != nil {
		return err
	}
	result.Proposals = append(result.Proposals, model.ExtractedProposal{Candidate: 0, RecordKind: "memory", RecordID: proposed.RecordID, VersionID: proposed.VersionID})
	return nil
}

func refreshDependencyIssue(input model.MemoryRefreshInput, output model.MemoryRefreshOutput) string {
	seen := map[string]bool{}
	for _, d := range output.Dependencies {
		if seen[d.RecordID] || !slices.Contains([]string{"assertion", "uncertain", "disputed"}, d.Label) {
			return "invalid_refresh_dependencies"
		}
		seen[d.RecordID] = true
		supplied := false
		for _, source := range input.Dependencies {
			if source.Scope != d.Scope || source.RecordID != d.RecordID {
				continue
			}
			for _, v := range source.Versions {
				if v.ID == d.VersionID && v.Digest == d.ContentDigest {
					supplied = true
				}
			}
		}
		if !supplied {
			return "dependency_not_supplied"
		}
	}
	if len(seen) != len(input.Dependencies) {
		return "dependency_omitted"
	}

	return ""
}
