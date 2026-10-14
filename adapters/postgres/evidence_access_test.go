package postgres_test

import (
	"context"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
)

type corruptEvidenceRead struct{ model.EvidenceStore }

func (c corruptEvidenceRead) Read(ctx context.Context, tenant string, ref model.ObjectRef) ([]byte, error) {
	return []byte("corrupt object bytes"), nil
}
func testEvidenceAccess(t *testing.T, ctx context.Context, l model.Ledger, store model.EvidenceStore, c model.Caller) {
	t.Helper()
	reader, _ := facts.New(l, denyPermission{permission: "can_read_evidence"}, store)
	q := model.EvidenceQuery{ID: "ev1", Scope: "case1", Purpose: "review"}
	metadata, err := reader.GetEvidenceMetadata(ctx, c, q)
	if err != nil || metadata.ID != q.ID {
		t.Fatal(metadata, err)
	}
	content, err := reader.GetEvidenceContent(ctx, c, q)
	if !errors.Is(err, model.ErrDenied) || len(content.Bytes) != 0 {
		t.Fatal("content permission bypassed", err)
	}
	full, _ := facts.New(l, allow{}, store)
	content, err = full.GetEvidenceContent(ctx, c, q)
	if err != nil || string(content.Bytes) != "I prefer tea." {
		t.Fatal(content, err)
	}
	for _, query := range []model.EvidenceQuery{{ID: q.ID, Scope: "wrong-scope", Purpose: q.Purpose}, {ID: "absent-id", Scope: q.Scope, Purpose: q.Purpose}} {
		if _, err = full.GetEvidenceMetadata(ctx, c, query); !errors.Is(err, model.ErrDenied) {
			t.Fatal("existence leak", err)
		}
	}
	other := c
	other.Tenant = "other-tenant"
	if _, err = full.GetEvidenceContent(ctx, other, q); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross tenant evidence", err)
	}
	corrupt, _ := facts.New(l, allow{}, corruptEvidenceRead{store})
	content, err = corrupt.GetEvidenceContent(ctx, c, q)
	if !errors.Is(err, model.ErrPrecondition) || len(content.Bytes) != 0 || content.Metadata.ID != "" {
		t.Fatal("unverified bytes disclosed", err)
	}
}
