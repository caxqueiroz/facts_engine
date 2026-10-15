package services

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"slices"
	"time"
)

func contextTx(tx model.Tx) (model.ContextTx, error) {
	t, ok := tx.(model.ContextTx)
	if !ok {
		return nil, model.ErrPrecondition
	}
	return t, nil
}
func (s *Service) contextAuthorization(ctx context.Context, c model.Caller, r model.ContextRequest, create bool) ([]model.AuthorizationDecision, error) {
	receipts := []model.AuthorizationDecision{}
	for _, scope := range r.Query.Scopes {
		permissions := []string{"can_read"}
		if create {
			permissions = append(permissions, "can_prepare_context")
		}
		if r.Query.KnowledgeRevision > 0 {
			permissions = append(permissions, "can_read_history")
		}
		for _, permission := range permissions {
			d, err := s.authorize(ctx, c, scope, permission, r.Command)
			if err != nil {
				return nil, err
			}
			receipts = append(receipts, d)
		}
	}
	return receipts, nil
}
func validContextRequest(r model.ContextRequest) bool {
	if !model.ValidID(r.Key) || !model.ValidID(r.Scope) || len(r.Query.Scopes) < 1 || len(r.Query.Scopes) > 8 || !slices.Contains(r.Query.Scopes, r.Scope) || r.Query.Purpose != r.Purpose {
		return false
	}
	seen := map[string]bool{}
	for _, scope := range r.Query.Scopes {
		if !model.ValidID(scope) || seen[scope] {
			return false
		}
		seen[scope] = true
	}
	return true
}
func renderContext(items []model.RecallItem) ([]byte, error) {
	return json.Marshal(struct {
		Schema string
		Facts  []model.RecallItem
	}{"facts-context-json-v1", items})
}
func (s *Service) PrepareContext(ctx context.Context, c model.Caller, r model.ContextRequest) (observationResult model.ContextManifest, observationErr error) {
	ctx, observation := s.telemetry.Start(ctx, "prepare.context")
	defer observation.End(&observationErr)
	if !validContextRequest(r) {
		return model.ContextManifest{}, model.ErrInvalid
	}
	r.Query.ValidAt = r.Query.ValidAt.UTC()
	if _, err := s.contextAuthorization(ctx, c, r, true); err != nil {
		return model.ContextManifest{}, err
	}
	if _, err := s.checkContextArchive(ctx, c.Tenant, 0); err != nil {
		return model.ContextManifest{}, err
	}
	// A retry resolves its committed manifest before calling retrieval, preserving
	// exact supplied bytes even after facts change or the index is unavailable.
	var prior *model.CommitResult
	digest := model.Digest(struct {
		Caller model.Caller
		Input  any
	}{c, r})
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		var err error
		prior, err = tx.Receipt(ctx, c, "prepare_context", r.Key, digest)
		return err
	})
	if err != nil {
		return model.ContextManifest{}, err
	}
	if prior != nil {
		return s.GetContextManifest(ctx, c, model.ContextQuery{ID: prior.ID, Purpose: r.Purpose})
	}
	var recall model.RecallResult
	var memories []model.MemoryRecallItem
	var order []model.VersionReference
	if r.IncludeMemories {
		recall, memories, order, err = s.recallContext(ctx, c, r.Query)
	} else {
		recall, err = s.Recall(ctx, c, r.Query)
	}
	if err != nil {
		return model.ContextManifest{}, err
	}
	describer, ok := s.retrieval.Index.(model.RetrievalDescriber)
	if !ok {
		return model.ContextManifest{}, model.ErrPrecondition
	}
	identity, err := describer.DescribeRetrieval(ctx, c.Tenant, recall.Generation)
	if err != nil {
		return model.ContextManifest{}, err
	}
	if identity.Generation != recall.Generation || !model.ValidID(identity.EmbeddingID) || identity.SearchVersion == "" || identity.RerankerID == "" || (identity.EmbeddingID == "none" && identity.Dimensions != 0) || (identity.EmbeddingID != "none" && identity.Dimensions < 1) {
		return model.ContextManifest{}, model.ErrPrecondition
	}
	rendered, err := renderContext(recall.Items)
	if r.IncludeMemories {
		rendered, err = renderMixedContext(recall.Items, memories, order)
	}
	if err != nil {
		return model.ContextManifest{}, err
	}
	if len(rendered) > 8<<20 {
		return model.ContextManifest{}, model.ErrPrecondition
	}
	receipts, err := s.contextAuthorization(ctx, c, r, true)
	if err != nil {
		return model.ContextManifest{}, err
	}
	m := model.ContextManifest{ID: id(), Tenant: c.Tenant, Scope: r.Scope, Caller: c, Request: r, CanonicalVersion: "context-manifest-v1", RedactionVersion: "facts-only-v1", QueryReference: model.Digest(r.Query), Retrieval: identity, Items: recall.Items, Rendered: rendered, RenderedDigest: model.BytesDigest(rendered), SelectionRevision: recall.LedgerRevision, IndexedRevision: recall.IndexedRevision, Incomplete: recall.Incomplete, Authorization: receipts}
	if r.IncludeMemories {
		m.CanonicalVersion = "context-manifest-v2"
		m.RedactionVersion = "facts-memories-v1"
		m.Memories = memories
		m.Order = order
		checks, err := s.contextMemoryAuthorization(ctx, c, m, r.Purpose, true)
		if err != nil {
			return model.ContextManifest{}, err
		}
		receipts = append(receipts, checks...)
		m.Authorization = receipts
	}
	m.Archive, err = s.checkContextArchive(ctx, c.Tenant, m.SelectionRevision)
	if err != nil {
		return model.ContextManifest{}, err
	}
	m.Digest = model.ContextDigest(m)
	result, err := s.mutate(ctx, c, r.Scope, "prepare_context", "can_prepare_context", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		for _, receipt := range receipts {
			if time.Since(receipt.CheckedAt) > 30*time.Second {
				return model.ErrUnavailable
			}
		}
		t, err := contextTx(tx)
		if err != nil {
			return err
		}
		if err = t.StageContext(ctx, m); err != nil {
			return err
		}
		out.ID = m.ID
		out.ContentDigest = m.Digest
		return nil
	})
	if err != nil {
		return model.ContextManifest{}, err
	}
	return s.GetContextManifest(ctx, c, model.ContextQuery{ID: result.ID, Purpose: r.Purpose})
}
func (s *Service) GetContextManifest(ctx context.Context, c model.Caller, q model.ContextQuery) (model.ContextManifest, error) {
	if !model.ValidID(c.Tenant) || !model.ValidID(c.Principal) || !model.ValidID(q.ID) || q.Purpose == "" {
		return model.ContextManifest{}, model.ErrInvalid
	}
	var m model.ContextManifest
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := contextTx(tx)
		if err != nil {
			return err
		}
		m, err = t.ContextManifest(ctx, q.ID)
		return err
	})
	if errors.Is(err, model.ErrNotFound) {
		return model.ContextManifest{}, model.ErrDenied
	}
	if err != nil {
		return model.ContextManifest{}, err
	}
	request := m.Request
	request.Purpose = q.Purpose
	if _, err = s.contextAuthorization(ctx, c, request, false); err != nil {
		return model.ContextManifest{}, err
	}
	if m.PayloadDisposed {
		if _, err := s.contextMemoryAuthorization(ctx, c, m, q.Purpose, false); err != nil {
			return model.ContextManifest{}, err
		}
		return model.ContextManifest{}, model.ErrPrecondition
	}
	if m.Tenant != c.Tenant || !slices.Contains([]string{"context-manifest-v1", "context-manifest-v2"}, m.CanonicalVersion) || model.ContextDigest(m) != m.Digest || model.BytesDigest(m.Rendered) != m.RenderedDigest {
		return model.ContextManifest{}, model.ErrPrecondition
	}
	if (m.CanonicalVersion == "context-manifest-v2") != m.Request.IncludeMemories || (!m.Request.IncludeMemories && (len(m.Memories) > 0 || len(m.Order) > 0)) {
		return model.ContextManifest{}, model.ErrPrecondition
	}
	if _, err = s.contextMemoryAuthorization(ctx, c, m, q.Purpose, false); err != nil {
		return model.ContextManifest{}, err
	}
	if _, err := s.checkContextArchive(ctx, c.Tenant, m.CreationRevision); err != nil {
		return model.ContextManifest{}, err
	}
	return m, nil
}

func renderMixedContext(facts []model.RecallItem, memories []model.MemoryRecallItem, order []model.VersionReference) ([]byte, error) {
	return json.Marshal(struct {
		Schema   string
		Facts    []model.RecallItem
		Memories []model.MemoryRecallItem
		Order    []model.VersionReference
	}{"facts-memories-context-json-v1", facts, memories, order})
}

func (s *Service) contextMemoryAuthorization(ctx context.Context, c model.Caller, m model.ContextManifest, purpose string, create bool) ([]model.AuthorizationDecision, error) {
	scopes := map[string]bool{}
	for _, item := range m.Memories {
		scopes[item.Key.Scope] = true
		for _, dependency := range item.Memory.Dependencies {
			scopes[dependency.Scope] = true
		}
	}
	ordered := []string{}
	for scope := range scopes {
		ordered = append(ordered, scope)
	}
	slices.Sort(ordered)
	receipts := []model.AuthorizationDecision{}
	for _, scope := range ordered {
		permissions := []string{"can_read", "can_read_history", "can_read_evidence"}
		if create {
			permissions = append(permissions, "can_prepare_context")
		}
		for _, permission := range permissions {
			d, err := s.authorize(ctx, c, scope, permission, model.Command{Purpose: purpose})
			if err != nil {
				return nil, err
			}
			receipts = append(receipts, d)
		}
	}
	return receipts, nil
}
