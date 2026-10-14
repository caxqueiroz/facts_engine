package services

import (
	"context"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

type conflictLedger struct {
	model.Ledger
	conflict error
}

func (l conflictLedger) Transact(context.Context, string, func(model.Tx) error) error {
	return l.conflict
}
func TestRevisionConflictDisclosureReauthorizes(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		checks := 0
		conflict := &model.RevisionConflict{RecordID: "record", ExpectedRevision: 2, CurrentRevision: 4}
		s := &Service{ledger: conflictLedger{conflict: conflict}, auth: waitAuth{check: func() bool { checks++; return !revoke || checks == 1 }}}
		result, err := s.mutate(context.Background(), model.Caller{Tenant: "tenant", Principal: "caller"}, "scope", "propose_fact", "can_propose", model.Command{Key: "command", Purpose: "review"}, nil, nil)
		var detail *model.RevisionConflict
		if revoke {
			if !errors.Is(err, model.ErrDenied) || errors.As(err, &detail) {
				t.Fatal("conflict disclosed after revocation", err)
			}
		} else if !errors.Is(err, model.ErrRevision) || !errors.As(err, &detail) || detail != conflict || err.Error() != model.ErrRevision.Error() {
			t.Fatal("conflict contract", err)
		}
		if checks != 2 || result != (model.CommitResult{}) {
			t.Fatal("disclosure checks or partial result", checks, result)
		}
	}
}
