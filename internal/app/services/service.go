package services

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/caxqueiroz/facts_engine/internal/telemetry"
	"github.com/caxqueiroz/facts_engine/model"
	"slices"
	"time"
	"unicode/utf8"
)

type Service struct {
	contextArchive      *model.ContextArchiveOptions
	telemetry           *telemetry.Observer
	operations          model.OperationsOptions
	redactionPolicies   map[string]bool
	attestationIdentity model.AttestationIdentityOptions
	extractors          map[string]model.ApprovedExtractor
	extractionLimits    model.ExtractionOptions
	reads               model.ReadOptions
	retrieval           model.RetrievalOptions
	ledger              model.Ledger
	auth                model.Authorizer
	evidence            model.EvidenceStore
	audit               model.AuditOptions
}

func New(l model.Ledger, a model.Authorizer, e model.EvidenceStore) (*Service, error) {
	if l == nil || a == nil || e == nil {
		return nil, model.ErrInvalid
	}
	return &Service{ledger: l, auth: a, evidence: e}, nil
}
func id() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", b)
}
func (s *Service) authorize(ctx context.Context, c model.Caller, scope, permission string, cmd model.Command) (observationResult model.AuthorizationDecision, observationErr error) {
	ctx, observation := s.telemetry.Start(ctx, "check.authorization")
	defer observation.End(&observationErr)
	if !model.ValidID(c.Tenant) || !model.ValidID(c.Principal) || !model.ValidID(scope) || cmd.Purpose == "" || len(cmd.Purpose) > 256 || (c.Initiator != "" && (!c.Service || !model.ValidID(c.Initiator))) {
		return model.AuthorizationDecision{}, model.ErrInvalid
	}
	req := model.AuthorizationRequest{Caller: c, Scope: scope, Permission: permission, Purpose: cmd.Purpose, CorrelationID: cmd.CorrelationID}
	d, err := s.auth.Check(ctx, req)
	if err != nil {
		return d, fmt.Errorf("authorization: %w", model.ErrUnavailable)
	}
	if !d.Allowed {
		return d, model.ErrDenied
	}
	if d.ModelID == "" || d.Consistency != "HIGHER_CONSISTENCY" || time.Since(d.CheckedAt) > 30*time.Second || d.CheckedAt.After(time.Now().Add(time.Second)) {
		return d, model.ErrUnavailable
	}
	d.Request = req
	d.ContextDigest = model.Digest(req)
	return d, nil
}
func (s *Service) mutate(ctx context.Context, c model.Caller, scope, op, permission string, cmd model.Command, input any, fn func(model.Tx, *model.CommitResult) error, commitChecks ...mutationCheck) (model.CommitResult, error) {
	var result model.CommitResult
	if !model.ValidID(cmd.Key) {
		return result, model.ErrInvalid
	}
	auth, err := s.authorize(ctx, c, scope, permission, cmd)
	if err != nil {
		return result, err
	}
	digest := model.Digest(struct {
		Caller model.Caller
		Input  any
	}{c, input})
	err = s.commitTransaction(ctx, c.Tenant, func(ctx context.Context, tx model.Tx) error {
		previous, err := tx.Receipt(ctx, c, op, cmd.Key, digest)
		if err != nil {
			return err
		}
		if previous != nil {
			for _, check := range commitChecks {
				if check.onReceipt != nil {
					if err := check.onReceipt(); err != nil {
						return err
					}
				}
			}
			result = *previous
			return nil
		}
		if op != "put_scope" {
			if err := tx.RequireScope(ctx, scope); err != nil {
				return err
			}
		}
		if err := fn(tx, &result); err != nil {
			return err
		}
		if time.Since(auth.CheckedAt) > 30*time.Second {
			return model.ErrUnavailable
		}
		eventInput := model.Event{RecordKind: result.RecordKind, ID: id(), EntityID: result.ID, Type: op, Scope: scope, RecordID: result.RecordID, VersionID: result.VersionID, StreamRevision: result.StreamRevision, Caller: c, Command: cmd, Authorization: auth}
		switch request := input.(type) {
		case memoryRefreshSweepCommand:
			eventInput.RefreshSweep = &model.MemoryRefreshSweep{Stream: request.Stream, ProfileID: request.ProfileID, ProfileDigest: request.ProfileDigest, ScheduledFor: request.ScheduledFor, Interval: request.Interval}

		case model.PredicateRequest:
			eventInput.DefinitionPredicate = request.Definition.Predicate
		case model.Supersession:
			eventInput.Reason = request.Reason
			eventInput.Affected = &request.Valid
			eventInput.ReplacedVersionIDs = request.ReplacedVersionIDs
		case model.LifecycleChange:
			eventInput.Reason = request.Reason
			if op != "reject_fact" && op != "reject_memory" {
				eventInput.Affected = &request.Valid
			}
		}
		event, err := tx.Append(ctx, eventInput)
		if err != nil {
			return err
		}
		for _, check := range commitChecks {
			if check.onCommit != nil {
				if err := check.onCommit(); err != nil {
					return err
				}
			}
		}
		result.LedgerRevision = event.LedgerRevision
		if result.ID == "" {
			result.ID = event.ID
		}
		return tx.SaveReceipt(ctx, c, op, cmd.Key, digest, result)
	})
	var conflict *model.RevisionConflict
	if errors.As(err, &conflict) {
		// A conflict may follow lock contention beyond the initial authorization
		// window. Recheck before disclosing the locked record revision.
		if _, checkErr := s.authorize(ctx, c, scope, permission, cmd); checkErr != nil {
			return model.CommitResult{}, checkErr
		}
		return model.CommitResult{}, conflict
	}
	return result, err
}
func (s *Service) RegisterAcceptancePolicy(ctx context.Context, c model.Caller, r model.PolicyRequest) (model.CommitResult, error) {
	p := r.Policy
	if p.Mode == "" {
		p.Mode = "manual"
		r.Policy = p
	}
	if err := validatePolicy(p); err != nil {
		return model.CommitResult{}, err
	}
	return s.mutate(ctx, c, p.Scope, "register_policy", "can_manage_definitions", r.Command, r, func(tx model.Tx, out *model.CommitResult) error { out.ID = p.ID; return tx.InsertPolicy(ctx, p) })
}
func (s *Service) RegisterPredicateVersion(ctx context.Context, c model.Caller, r model.PredicateRequest) (model.CommitResult, error) {
	d := r.Definition
	if !model.ValidID(d.Predicate) || !model.ValidID(d.Version) || !model.ValidID(d.PolicyID) || d.Classification == "" || d.ValidateSchema() != nil || !slices.Contains([]model.ValueKind{model.Text, model.Boolean, model.Integer, model.Decimal, model.Date, model.Instant, model.Entity}, d.Kind) {
		return model.CommitResult{}, model.ErrInvalid
	}
	return s.mutate(ctx, c, d.Scope, "register_predicate", "can_manage_definitions", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		if _, err := tx.Policy(ctx, d.Scope, d.PolicyID); err != nil {
			return err
		}
		active, err := tx.ActivePredicate(ctx, d.Scope, d.Predicate)
		if errors.Is(err, model.ErrNotFound) {
			if d.Migration != nil {
				return model.ErrPrecondition
			}
		} else if err != nil {
			return err
		} else if d.Migration == nil || d.Migration.FromVersion != active {
			return model.ErrPrecondition
		}
		out.ID = d.Version
		return tx.InsertPredicate(ctx, d)
	})
}
func (s *Service) verify(ctx context.Context, tenant string, e model.Evidence) ([]byte, error) {
	b, _, err := s.verifyEvidenceIntegrity(ctx, tenant, e)
	return b, err
}

// evidenceDependencyFailure preserves cancellation and temporary unavailability
// without exposing storage error text or treating an outage as missing evidence.
func evidenceDependencyFailure(ctx context.Context, err error) error {
	if interrupted := ctx.Err(); interrupted != nil {
		return interrupted
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, model.ErrUnavailable) {
		return model.ErrUnavailable
	}
	return nil
}

func (s *Service) verifyEvidenceIntegrity(ctx context.Context, tenant string, e model.Evidence) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	b, err := s.evidence.Read(ctx, tenant, e.Object)
	if interrupted := evidenceDependencyFailure(ctx, err); interrupted != nil {
		return nil, false, interrupted
	}
	if err != nil {
		return nil, false, model.ErrPrecondition
	}
	if int64(len(b)) != e.Object.Size || model.BytesDigest(b) != e.Object.Digest {
		return nil, true, model.ErrPrecondition
	}
	return b, false, nil
}
func (s *Service) RegisterEvidence(ctx context.Context, c model.Caller, r model.EvidenceRequest) (model.CommitResult, error) {
	e := r.Evidence
	if e.Derivation != nil || !model.ValidID(e.ID) || e.SourceSystem == "" || e.SourceObject == "" || e.SourceRevision == "" || e.Speaker == "" || e.SourceClass == "" || e.MediaType == "" || e.Classification == "" || e.RetentionPolicy == "" || e.SourceTime.IsZero() || e.Object.Locator == "" || e.Object.Version == "" || e.Object.Size < 0 || e.Object.Size > 16<<20 {
		return model.CommitResult{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, e.Scope, "can_propose", r.Command); err != nil {
		return model.CommitResult{}, err
	}
	if _, err := s.verify(ctx, c.Tenant, e); err != nil {
		return model.CommitResult{}, err
	}
	return s.mutate(ctx, c, e.Scope, "register_evidence", "can_propose", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		eid, err := tx.InsertEvidence(ctx, e)
		out.ID = eid
		return err
	})
}
func (s *Service) loadEvidence(ctx context.Context, c model.Caller, scope string, ids []string) (observationResult map[string][]byte, observationErr error) {
	ctx, observation := s.telemetry.Start(ctx, "load.evidence")
	defer observation.End(&observationErr)
	evidence := make([]model.Evidence, 0, len(ids))
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		for _, id := range ids {
			e, err := tx.Evidence(ctx, id)
			if err != nil {
				return err
			}
			if e.Scope != scope {
				return model.ErrDenied
			}
			evidence = append(evidence, e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	data := map[string][]byte{}
	for _, e := range evidence {
		b, err := s.verify(ctx, c.Tenant, e)
		if err != nil {
			return nil, err
		}
		data[e.ID] = b
	}
	return data, nil
}
func (s *Service) ProposeFact(ctx context.Context, c model.Caller, r model.FactProposal) (observationResult model.CommitResult, observationErr error) {
	ctx, observation := s.telemetry.Start(ctx, "propose.fact")
	defer observation.End(&observationErr)
	if !model.ValidID(r.Key.Subject) || !model.ValidID(r.Key.Predicate) || !model.ValidID(r.SchemaVersion) || (r.Key.Discriminator != "" && !model.ValidID(r.Key.Discriminator)) || r.ExpectedRevision < 0 || r.Value.Validate() != nil || r.Valid.Validate() != nil || len(r.Sources) == 0 || len(r.Sources) > 64 {
		return model.CommitResult{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, r.Key.Scope, "can_propose", r.Command); err != nil {
		return model.CommitResult{}, err
	}
	ids := []string{}
	for _, span := range r.Sources {
		ids = append(ids, span.EvidenceID)
	}
	data, err := s.loadEvidence(ctx, c, r.Key.Scope, ids)
	if err != nil {
		return model.CommitResult{}, err
	}
	for _, span := range r.Sources {
		b := data[span.EvidenceID]
		if span.Start < 0 || span.End <= span.Start || span.End > len(b) || !utf8.Valid(b[span.Start:span.End]) || string(b[span.Start:span.End]) != span.Quote {
			return model.CommitResult{}, model.ErrInvalid
		}
	}
	return s.mutate(ctx, c, r.Key.Scope, "propose_fact", "can_propose", r.Command, r, func(tx model.Tx, out *model.CommitResult) error { return s.stageFactProposal(ctx, tx, c, r, "", out) })
}
func (s *Service) stageFactProposal(ctx context.Context, tx model.Tx, c model.Caller, r model.FactProposal, attemptID string, out *model.CommitResult) error {
	if err := validateDerivativeSources(ctx, tx, r.Sources); err != nil {
		return err
	}
	if err := tx.RequireSubject(ctx, r.Key.Scope, r.Key.Subject); err != nil {
		return err
	}
	active, err := tx.ActivePredicate(ctx, r.Key.Scope, r.Key.Predicate)
	if err != nil {
		return err
	}
	if active != r.SchemaVersion {
		return model.ErrPrecondition
	}
	d, err := tx.Predicate(ctx, r.Key.Scope, r.Key.Predicate, r.SchemaVersion)
	if err != nil {
		return err
	}
	if d.ValidateFact(r.Value, r.Valid) != nil {
		return model.ErrInvalid
	}
	rec, err := tx.LockRecord(ctx, r.Key)
	if err != nil {
		return err
	}
	if rec.Revision != r.ExpectedRevision {
		return &model.RevisionConflict{RecordID: rec.ID, ExpectedRevision: r.ExpectedRevision, CurrentRevision: rec.Revision}
	}
	v := model.Version{ExtractionAttemptID: attemptID, ID: id(), RecordID: rec.ID, Proposal: r, Proposer: c, PolicyID: d.PolicyID}
	v.Digest = model.Digest(struct {
		Key     model.FactKey
		Value   model.Value
		Valid   model.Interval
		Schema  string
		Sources []model.SourceSpan
	}{r.Key, r.Value, r.Valid, r.SchemaVersion, r.Sources})
	if err := tx.InsertVersion(ctx, v); err != nil {
		return err
	}
	out.ID = v.ID
	out.ContentDigest = v.Digest
	out.RecordID = rec.ID
	out.VersionID = v.ID
	out.StreamRevision = rec.Revision + 1
	return tx.SetRevision(ctx, rec.ID, out.StreamRevision)
}

func validKind(k string) bool {
	return slices.Contains([]string{"client_confirmation", "source_verification", "application_validation", "human_review"}, k)
}
func (s *Service) RecordAttestation(ctx context.Context, c model.Caller, r model.AttestationRequest) (model.CommitResult, error) {
	return s.recordAttestation(ctx, c, r, r, nil)
}
func (s *Service) recordAttestation(ctx context.Context, c model.Caller, r model.AttestationRequest, input any, delegation *model.AttestationDelegation) (model.CommitResult, error) {
	a := r.Attestation
	// Server-owned identity metadata never comes from an attestation request.
	if a.RecordedAt != nil || a.ReviewAuthorization != nil || a.Recorder != nil || a.Delegation != nil || a.EvidenceAuthorization != nil || !validConfirmation(a) || (a.Confirmation != nil && a.Confirmation.RespondedAt.After(time.Now().Add(time.Second))) || !validKind(a.Kind) || !model.ValidID(a.ID) || (delegation == nil && a.Attestor != c) || a.ContentDigest == "" || (a.ExpiresAt != nil && !a.ExpiresAt.After(time.Now())) {
		return model.CommitResult{}, model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, a.Scope, "can_attest", r.Command); err != nil {
		return model.CommitResult{}, err
	}
	if a.Kind == "human_review" {
		if c.Service || c.Initiator != "" {
			return model.CommitResult{}, model.ErrInvalid
		}
		check, err := s.authorize(ctx, c, a.Scope, "can_approve", r.Command)
		if err != nil {
			return model.CommitResult{}, err
		}
		a.ReviewAuthorization = &check
	}
	if a.Confirmation != nil {
		check, err := s.authorize(ctx, c, a.Scope, "can_read_evidence", r.Command)
		if err != nil {
			return model.CommitResult{}, err
		}
		a.EvidenceAuthorization = &check
	}
	if err := s.verifyAttestationEvidence(ctx, c, a); err != nil {
		return model.CommitResult{}, err
	}
	a.Recorder = &c
	a.Delegation = delegation
	return s.mutate(ctx, c, a.Scope, "record_attestation", "can_attest", r.Command, input, func(tx model.Tx, out *model.CommitResult) error {
		if a.EvidenceAuthorization != nil && time.Since(a.EvidenceAuthorization.CheckedAt) > 30*time.Second {
			return model.ErrUnavailable
		}
		if delegation != nil {
			if !delegation.Receipt.ExpiresAt.After(time.Now()) || time.Since(delegation.Receipt.VerifiedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
			for _, check := range delegation.Authorization {
				if time.Since(check.CheckedAt) > 30*time.Second {
					return model.ErrUnavailable
				}
			}
		}
		record, err := tx.Record(ctx, a.RecordID)
		if err != nil {
			return err
		}
		if record.Key.Scope != a.Scope {
			return model.ErrDenied
		}
		if record.Kind == "memory" {
			stored := r
			stored.Attestation = a
			return s.recordMemoryAttestation(ctx, tx, c, stored, out)
		}
		if err := tx.RequireSubject(ctx, record.Key.Scope, record.Key.Subject); err != nil {
			return err
		}
		v, err := tx.Version(ctx, a.RecordID, a.VersionID)
		if err != nil {
			return err
		}
		if v.Proposal.Key.Scope != a.Scope || v.Digest != a.ContentDigest {
			return model.ErrPrecondition
		}
		p, err := tx.Policy(ctx, a.Scope, v.PolicyID)
		if err != nil {
			return err
		}
		if err := attestationPolicyChecks(p, a, v.Proposer); err != nil {
			return err
		}
		out.ID = a.ID
		return tx.InsertAttestation(ctx, a)
	}, freshnessCheck(func() error { return freshAttestationIdentity(a, time.Now()) }))
}
func (s *Service) WithdrawAttestation(ctx context.Context, c model.Caller, r model.Withdrawal) (model.CommitResult, error) {
	return s.mutate(ctx, c, r.Scope, "withdraw_attestation", "can_attest", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		result, err := tx.WithdrawAttestation(ctx, r.AttestationID, r.Scope, c)
		*out = result
		return err
	})
}
func prerequisites(p model.AcceptancePolicy, v model.Version, as []model.Attestation, now time.Time) ([]string, error) {
	return versionPrerequisites(p, v.ID, v.Digest, as, now)
}
func versionPrerequisites(p model.AcceptancePolicy, versionID, digest string, as []model.Attestation, now time.Time) ([]string, error) {
	ids := []string{}
	for _, req := range p.Required {
		found := false
		for _, a := range as {
			if validConfirmation(a) && (a.Confirmation == nil || !a.Confirmation.RespondedAt.After(now)) && a.Kind == req.Kind && a.VersionID == versionID && a.ContentDigest == digest && slices.Contains(req.Principals, a.Attestor.Principal) && attestationEligible(req, a, now) {
				ids = append(ids, a.ID)
				found = true
				break
			}
		}
		if !found {
			return nil, model.ErrPrecondition
		}
	}
	return ids, nil
}
func (s *Service) ApproveFact(ctx context.Context, c model.Caller, r model.Approval) (observationResult model.CommitResult, observationErr error) {
	ctx, observation := s.telemetry.Start(ctx, "approve.fact")
	defer observation.End(&observationErr)
	return s.accept(ctx, c, r, false)
}
func (s *Service) AcceptFact(ctx context.Context, c model.Caller, r model.Approval) (observationResult model.CommitResult, observationErr error) {
	ctx, observation := s.telemetry.Start(ctx, "accept.fact")
	defer observation.End(&observationErr)
	return s.accept(ctx, c, r, true)
}
func (s *Service) accept(ctx context.Context, c model.Caller, r model.Approval, automatic bool) (model.CommitResult, error) {
	op, permission := "approve_fact", "can_approve"
	if automatic {
		op, permission = "accept_fact", "can_accept"
	}
	if _, err := s.authorize(ctx, c, r.Scope, permission, r.Command); err != nil {
		return model.CommitResult{}, err
	}
	var preliminary model.Version
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		v, err := tx.Version(ctx, r.RecordID, r.VersionID)
		preliminary = v
		return err
	})
	if err != nil {
		return model.CommitResult{}, err
	}
	if preliminary.Proposal.Key.Scope != r.Scope {
		return model.CommitResult{}, model.ErrDenied
	}
	ids := []string{}
	for _, span := range preliminary.Proposal.Sources {
		ids = append(ids, span.EvidenceID)
	}
	if _, err := s.loadEvidence(ctx, c, r.Scope, ids); err != nil {
		return model.CommitResult{}, err
	}
	verified, err := s.verifiedPrerequisites(ctx, c, r.Scope, preliminary.ID, preliminary.Digest, preliminary.PolicyID, false)
	if err != nil {
		return model.CommitResult{}, err
	}
	var commitPolicy *model.AcceptancePolicy
	var commitIDs []string
	return s.mutate(ctx, c, r.Scope, op, permission, r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		rec, err := tx.LockRecord(ctx, preliminary.Proposal.Key)
		if err != nil {
			return err
		}
		if rec.ID != r.RecordID {
			return model.ErrNotFound
		}
		if rec.Revision != r.ExpectedRevision {
			return &model.RevisionConflict{RecordID: rec.ID, ExpectedRevision: r.ExpectedRevision, CurrentRevision: rec.Revision}
		}
		v, err := tx.Version(ctx, r.RecordID, r.VersionID)
		if err != nil {
			return err
		}
		p, attIDs, err := s.acceptanceChecks(ctx, tx, c, rec, v, automatic)
		if err != nil {
			return err
		}
		if err := requireVerifiedPrerequisites(attIDs, verified); err != nil {
			return err
		}
		commitPolicy = &p
		commitIDs = attIDs
		accepted, err := tx.Acceptances(ctx, rec.ID, 0)
		if err != nil {
			return err
		}
		for _, a := range accepted {
			if a.State != "retracted" && a.Valid.Overlaps(v.Proposal.Valid) {
				return model.ErrPrecondition
			}
		}
		if err := tx.InsertAcceptance(ctx, rec.ID, model.Acceptance{VersionID: v.ID, Valid: v.Proposal.Valid, PolicyID: p.ID, AttestationIDs: attIDs}); err != nil {
			return err
		}
		out.ID = v.ID
		out.ContentDigest = v.Digest
		out.RecordID = rec.ID
		out.VersionID = v.ID
		out.StreamRevision = rec.Revision + 1
		return tx.SetRevision(ctx, rec.ID, out.StreamRevision)
	}, commitOnlyCheck(func() error {
		return currentBoundPrerequisites(commitPolicy, preliminary.ID, preliminary.Digest, commitIDs, verified)
	}))
}

func NewWithAudit(l model.Ledger, a model.Authorizer, e model.EvidenceStore, options model.AuditOptions) (*Service, error) {
	s, err := New(l, a, e)
	if err != nil {
		return nil, err
	}
	s.audit = options
	return s, nil
}
