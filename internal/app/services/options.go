package services

import (
	"github.com/caxqueiroz/facts_engine/internal/telemetry"
	"github.com/caxqueiroz/facts_engine/model"
)

func NewWithOptions(l model.Ledger, a model.Authorizer, e model.EvidenceStore, o model.EngineOptions) (*Service, error) {
	s, err := New(l, a, e)
	if err != nil {
		return nil, err
	}
	s.telemetry, err = telemetry.New(o.Telemetry)
	if err != nil {
		return nil, err
	}
	if err = configureAttestationIdentity(s, o.AttestationIdentity); err != nil {
		return nil, err
	}
	if len(o.RedactionPolicyVersions) > 64 {
		return nil, model.ErrInvalid
	}
	s.redactionPolicies = map[string]bool{}
	for _, version := range o.RedactionPolicyVersions {
		if !model.ValidID(version) || s.redactionPolicies[version] {
			return nil, model.ErrInvalid
		}
		s.redactionPolicies[version] = true
	}
	if err = configureExtraction(s, o.Extraction); err != nil {
		return nil, err
	}
	if o.ContextArchive != nil {
		policy := *o.ContextArchive
		if !model.ValidID(policy.Stream) || policy.MaxPendingRevisions < 0 || policy.Progress == nil {
			return nil, model.ErrInvalid
		}
		s.contextArchive = &policy
	}
	s.projectionCleanup = o.ProjectionCleanup
	s.audit = o.Audit
	if o.Retrieval.Index != nil || o.Retrieval.Progress != nil || o.Retrieval.Registry != nil || o.Retrieval.Generation != "" {
		retrieval, err := NewWithRetrieval(l, a, e, o.Retrieval)
		if err != nil {
			return nil, err
		}
		s.retrieval = retrieval.retrieval
	}
	if o.Reads.PaginationKey != nil || o.Reads.TokenLifetime != 0 {
		reads, err := NewWithReads(l, a, e, o.Reads)
		if err != nil {
			return nil, err
		}
		s.reads = reads.reads
	}
	if o.Operations.Scope != "" || o.Operations.Store != nil || o.Operations.Index != nil || o.Operations.Progress != nil {
		if !model.ValidID(o.Operations.Scope) || o.Operations.Store == nil || o.Operations.Index == nil || o.Operations.Progress == nil {
			return nil, model.ErrInvalid
		}
		s.operations = o.Operations
	}
	return s, nil
}
