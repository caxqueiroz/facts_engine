package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

type interruptingEvidence struct {
	model.EvidenceStore
	read func(context.Context) ([]byte, error)
}

func (s interruptingEvidence) Read(ctx context.Context, _ string, _ model.ObjectRef) ([]byte, error) {
	return s.read(ctx)
}
func TestEvidenceVerificationPreservesInterruption(t *testing.T) {
	body := []byte("private evidence")
	evidence := model.Evidence{ID: "evidence", Object: model.ObjectRef{Size: int64(len(body)), Digest: model.BytesDigest(body)}}
	for _, want := range []error{context.Canceled, context.DeadlineExceeded, model.ErrUnavailable} {
		s := &Service{evidence: interruptingEvidence{read: func(context.Context) ([]byte, error) { return body, fmt.Errorf("PRIVATE storage: %w", want) }}}
		got, mismatch, err := s.verifyEvidenceIntegrity(context.Background(), "tenant", evidence)
		if err != want || len(got) != 0 || mismatch {
			t.Fatal("interruption lost or exposed content", got, mismatch, err)
		}
		raw, _ := json.Marshal(evidence)
		entries, gaps, err := s.prepareAuditEntries(context.Background(), model.Caller{Tenant: "tenant"}, model.AuditExportRequest{IncludeEvidence: true}, []model.AuditEntry{{Kind: "evidence", ID: evidence.ID, Payload: raw}})
		if !errors.Is(err, want) || len(entries) != 0 || len(gaps) != 0 {
			t.Fatal("interruption downgraded to audit gap", entries, gaps, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	s := &Service{evidence: interruptingEvidence{read: func(context.Context) ([]byte, error) { called = true; return body, nil }}}
	if _, _, err := s.verifyEvidenceIntegrity(ctx, "tenant", evidence); !errors.Is(err, context.Canceled) || called {
		t.Fatal("canceled read started", err, called)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	s.evidence = interruptingEvidence{read: func(context.Context) ([]byte, error) { cancel(); return body, nil }}
	if got, mismatch, err := s.verifyEvidenceIntegrity(ctx, "tenant", evidence); !errors.Is(err, context.Canceled) || len(got) != 0 || mismatch {
		t.Fatal("late cancellation ignored", got, mismatch, err)
	}
}
