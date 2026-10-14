package services

import "github.com/caxqueiroz/facts_engine/model"

func NewWithOptions(l model.Ledger, a model.Authorizer, e model.EvidenceStore, o model.EngineOptions) (*Service, error) {
	s, err := New(l, a, e)
	if err != nil {
		return nil, err
	}
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
	return s, nil
}
