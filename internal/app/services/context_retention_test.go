package services

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

type retainedContextLedger struct {
	model.Ledger
	manifest model.ContextManifest
}

func (l retainedContextLedger) Transact(ctx context.Context, _ string, fn func(model.Tx) error) error {
	return fn(retainedContextTx{manifest: l.manifest})
}

type retainedContextTx struct {
	model.ContextTx
	manifest model.ContextManifest
}

func (t retainedContextTx) ContextManifest(context.Context, string) (model.ContextManifest, error) {
	return t.manifest, nil
}

func TestDisposedContextReadAuthorization(t *testing.T) {
	m := model.ContextManifest{ID: "manifest", Tenant: "bank", CanonicalVersion: "context-manifest-v2", Request: model.ContextRequest{IncludeMemories: true, Query: model.RecallRequest{Scopes: []string{"scope"}}}, Memories: []model.MemoryRecallItem{{Key: model.MemoryKey{Scope: "scope"}, Memory: model.MemoryResult{Body: "private", Dependencies: []model.MemoryDependency{{Scope: "dependency"}}}}}, Rendered: []byte("private")}
	m.RenderedDigest = model.BytesDigest(m.Rendered)
	m.Digest = model.ContextDigest(m)
	// A disposal flag must also fail closed on an otherwise valid full manifest.
	m.PayloadDisposed = true
	for _, manifest := range []model.ContextManifest{m, model.ContextTombstone(m)} {
		for _, denied := range []string{"", "scope", "dependency"} {
			s := &Service{ledger: retainedContextLedger{manifest: manifest}, auth: contextPermissionAuthorizer{denyScope: denied, denyPermission: "can_read"}}
			got, err := s.GetContextManifest(context.Background(), model.Caller{Tenant: "bank", Principal: "reader"}, model.ContextQuery{ID: "manifest", Purpose: "review"})
			want := model.ErrPrecondition
			if denied != "" {
				want = model.ErrDenied
			}
			if !errors.Is(err, want) || !reflect.DeepEqual(got, model.ContextManifest{}) {
				t.Fatalf("scope %q: got %v, %v", denied, got, err)
			}
		}
	}
}
