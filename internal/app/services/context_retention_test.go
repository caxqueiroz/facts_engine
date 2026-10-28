package services

import (
	"context"
	"encoding/json"
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
	m := model.ContextManifest{ID: "manifest", Tenant: "org", CanonicalVersion: "context-manifest-v2", Request: model.ContextRequest{IncludeMemories: true, Query: model.RecallRequest{Scopes: []string{"scope"}}}, Memories: []model.MemoryRecallItem{{Key: model.MemoryKey{Scope: "scope"}, Memory: model.MemoryResult{Body: "private", Dependencies: []model.MemoryDependency{{Scope: "dependency"}}}}}, Rendered: []byte("private")}
	m.RenderedDigest = model.BytesDigest(m.Rendered)
	m.Digest = model.ContextDigest(m)
	// A disposal flag must also fail closed on an otherwise valid full manifest.
	m.PayloadDisposed = true
	for _, manifest := range []model.ContextManifest{m, model.ContextTombstone(m)} {
		for _, denied := range []string{"", "scope", "dependency"} {
			s := &Service{ledger: retainedContextLedger{manifest: manifest}, auth: contextPermissionAuthorizer{denyScope: denied, denyPermission: "can_read"}}
			got, err := s.GetContextManifest(context.Background(), model.Caller{Tenant: "org", Principal: "reader"}, model.ContextQuery{ID: "manifest", Purpose: "review"})
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

func TestDisposedContextAuditBoundaries(t *testing.T) {
	m := model.ContextTombstone(model.ContextManifest{ID: "manifest", Request: model.ContextRequest{Query: model.RecallRequest{Scopes: []string{"scope"}}}, Memories: []model.MemoryRecallItem{{Key: model.MemoryKey{Scope: "scope"}, Memory: model.MemoryResult{Body: "private", Dependencies: []model.MemoryDependency{{Scope: "scope", Label: "assertion"}}}}}, Rendered: []byte("private")})
	s := &Service{}
	for _, crossScope := range []bool{false, true} {
		if crossScope {
			m.Memories[0].Memory.Dependencies[0].Scope = "other"
		}
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		entries, gaps, err := s.prepareAuditEntries(context.Background(), model.Caller{Tenant: "org", Principal: "reader"}, model.AuditExportRequest{Scope: "scope", IncludeEvidence: false}, []model.AuditEntry{{Kind: "context", ID: m.ID, Payload: raw}})
		if err != nil {
			t.Fatal(err)
		}
		if crossScope {
			if len(entries) != 0 || len(gaps) != 1 || gaps[0].Reason != "outside_export_scope" {
				t.Fatal("cross-scope metadata disclosed", entries, gaps)
			}
		} else if len(entries) != 1 || len(gaps) != 1 || gaps[0].Kind != "context_content" || gaps[0].Reason != "disposed_under_retention_policy" {
			t.Fatal("disposed context coverage missing", entries, gaps)
		}
	}
}
