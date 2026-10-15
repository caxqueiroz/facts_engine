package services

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

type extractionDisposalTestTx struct {
	model.RetentionTx
	receipt                   *model.CommitResult
	digest                    string
	disposed                  int
	event                     model.RetentionEvent
	afterDispose, afterAppend func()
}

func (t *extractionDisposalTestTx) Receipt(_ context.Context, _ model.Caller, _, _, digest string) (*model.CommitResult, error) {
	if t.receipt != nil && t.digest != digest {
		return nil, model.ErrConflict
	}
	t.digest = digest
	return t.receipt, nil
}
func (*extractionDisposalTestTx) RequireScope(context.Context, string) error { return nil }
func (t *extractionDisposalTestTx) DisposeExtraction(context.Context, model.ExtractionDisposalRequest) error {
	t.disposed++
	if t.afterDispose != nil {
		t.afterDispose()
	}
	return nil
}
func (t *extractionDisposalTestTx) StageRetention(_ context.Context, e model.RetentionEvent) error {
	t.event = e
	return nil
}
func (t *extractionDisposalTestTx) Append(_ context.Context, e model.Event) (model.Event, error) {
	if t.afterAppend != nil {
		t.afterAppend()
	}
	e.LedgerRevision = 7
	return e, nil
}
func (t *extractionDisposalTestTx) SaveReceipt(_ context.Context, _ model.Caller, _, _, _ string, r model.CommitResult) error {
	t.receipt = &r
	return nil
}

type extractionDisposalTestLedger struct {
	model.Ledger
	tx extractionDisposalTestTx
}

func (l *extractionDisposalTestLedger) Transact(_ context.Context, _ string, f func(model.Tx) error) error {
	candidate := l.tx
	if err := f(&candidate); err != nil {
		return err
	}
	l.tx = candidate
	return nil
}

type extractionDisposalTestAuth struct {
	denied   string
	requests []model.AuthorizationRequest
	denyAt   int
}

func (a *extractionDisposalTestAuth) Check(_ context.Context, r model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	a.requests = append(a.requests, r)
	return model.AuthorizationDecision{Allowed: r.Scope != a.denied && (a.denyAt == 0 || len(a.requests) < a.denyAt), ModelID: "model", Consistency: "HIGHER_CONSISTENCY", CheckedAt: time.Now()}, nil
}
func extractionDisposalFixture() (*Service, *extractionDisposalTestLedger, *extractionDisposalTestAuth, model.Caller, model.ExtractionDisposalRequest) {
	l := &extractionDisposalTestLedger{}
	a := &extractionDisposalTestAuth{}
	r := model.ExtractionDisposalRequest{Command: model.Command{Key: "dispose", Purpose: "retention"}, JobID: "job", Reason: "expired", Bindings: []model.ExtractionRetentionBinding{
		{Scope: "z", JobPolicyID: "job-policy", RequestPolicyID: "request-policy", ResponsePolicyID: "response-policy", Jurisdiction: "sg"},
		{Scope: "a", JobPolicyID: "job-policy", RequestPolicyID: "request-policy", ResponsePolicyID: "response-policy", Jurisdiction: "sg"},
	}}
	return &Service{ledger: l, auth: a}, l, a, model.Caller{Tenant: "tenant", Principal: "operator"}, r
}
func TestExtractionDisposalAuthorizationAndReplay(t *testing.T) {
	s, l, a, c, r := extractionDisposalFixture()
	original := append([]model.ExtractionRetentionBinding(nil), r.Bindings...)
	first, err := s.DisposeExtraction(context.Background(), c, r)
	if err != nil || first.ID == "" || first.LedgerRevision != 7 || l.tx.disposed != 1 {
		t.Fatal(first, err, l.tx.disposed)
	}
	if !reflect.DeepEqual(r.Bindings, original) {
		t.Fatal("mutated caller binding order")
	}
	e := l.tx.event
	if e.Kind != "dispose_extraction" || e.TargetID != r.JobID || e.Scope != "a" || e.Caller != c || e.ExtractionDisposal == nil || e.ExtractionDisposal.Bindings[0].Scope != "a" || len(e.AdditionalAuthorization) != 2 {
		t.Fatal(e)
	}
	for _, req := range a.requests {
		if req.Permission != "can_dispose" || req.Purpose != r.Purpose || req.Caller != c {
			t.Fatal(req)
		}
	}
	r.Bindings[0], r.Bindings[1] = r.Bindings[1], r.Bindings[0]
	replay, err := s.DisposeExtraction(context.Background(), c, r)
	if err != nil || !reflect.DeepEqual(first, replay) || l.tx.disposed != 1 {
		t.Fatal(replay, err, l.tx.disposed)
	}
	r.Bindings[0].ResponsePolicyID = "changed"
	if _, err = s.DisposeExtraction(context.Background(), c, r); !errors.Is(err, model.ErrConflict) {
		t.Fatal("changed policy replay", err)
	}
	r.Bindings[0].ResponsePolicyID = "response-policy"
	a.denied = "z"
	if _, err = s.DisposeExtraction(context.Background(), c, r); !errors.Is(err, model.ErrDenied) || l.tx.disposed != 1 {
		t.Fatal("revoked replay", err)
	}
}
func TestExtractionDisposalRevocationRollsBack(t *testing.T) {
	for _, phase := range []string{"initial", "after-disposal", "before-commit"} {
		t.Run(phase, func(t *testing.T) {
			s, l, a, c, r := extractionDisposalFixture()
			revoke := func() { a.denied = "z" }
			switch phase {
			case "initial":
				revoke()
			case "after-disposal":
				l.tx.afterDispose = revoke
			case "before-commit":
				l.tx.afterAppend = revoke
			}
			if _, err := s.DisposeExtraction(context.Background(), c, r); !errors.Is(err, model.ErrDenied) {
				t.Fatal(err)
			}
			if l.tx.disposed != 0 || l.tx.receipt != nil || l.tx.event.ID != "" {
				t.Fatal("partial mutation committed")
			}
		})
	}
}
func TestExtractionDisposalRejectsInvalidBindingsBeforeAuthorization(t *testing.T) {
	changes := []func(*model.ExtractionDisposalRequest){
		func(r *model.ExtractionDisposalRequest) { r.JobID = "" },
		func(r *model.ExtractionDisposalRequest) { r.Reason = "" },
		func(r *model.ExtractionDisposalRequest) { r.Bindings = nil },
		func(r *model.ExtractionDisposalRequest) { r.Bindings[0].Scope = r.Bindings[1].Scope },
		func(r *model.ExtractionDisposalRequest) { r.Bindings[0].JobPolicyID = "" },
		func(r *model.ExtractionDisposalRequest) { r.Bindings[0].RequestPolicyID = "" },
		func(r *model.ExtractionDisposalRequest) { r.Bindings[0].ResponsePolicyID = "" },
		func(r *model.ExtractionDisposalRequest) { r.Bindings[0].Jurisdiction = "" },
		func(r *model.ExtractionDisposalRequest) { r.Bindings = make([]model.ExtractionRetentionBinding, 257) },
	}
	for i, change := range changes {
		s, l, a, c, r := extractionDisposalFixture()
		change(&r)
		if _, err := s.DisposeExtraction(context.Background(), c, r); !errors.Is(err, model.ErrInvalid) || len(a.requests) != 0 || l.tx.disposed != 0 {
			t.Fatal(i, err, a.requests)
		}
	}
}
func TestExtractionDisposalAuditWithholdsOtherScopeBindings(t *testing.T) {
	for _, crossScope := range []bool{false, true} {
		bindings := []model.ExtractionRetentionBinding{{Scope: "scope"}}
		if crossScope {
			bindings = append(bindings, model.ExtractionRetentionBinding{Scope: "private"})
		}
		raw, err := json.Marshal(model.RetentionEvent{Kind: "dispose_extraction", ExtractionDisposal: &model.ExtractionDisposalRequest{Bindings: bindings}})
		if err != nil {
			t.Fatal(err)
		}
		s := &Service{}
		out, _, err := s.prepareAuditEntries(context.Background(), model.Caller{}, model.AuditExportRequest{Scope: "scope"}, []model.AuditEntry{{Kind: "retention", ID: "event", Payload: raw}})
		if err != nil || (len(out) == 1) == crossScope {
			t.Fatal(crossScope, out, err)
		}
	}
}

func TestExtractionDisposalRechecksAuthorizationWhenReturningReceipt(t *testing.T) {
	s, l, a, c, r := extractionDisposalFixture()
	if _, err := s.DisposeExtraction(context.Background(), c, r); err != nil {
		t.Fatal(err)
	}
	a.denyAt = len(a.requests) + 5 // Initial scopes, mutation scope, then both receipt scopes.
	if _, err := s.DisposeExtraction(context.Background(), c, r); !errors.Is(err, model.ErrDenied) {
		t.Fatal("receipt disclosed after revocation", err)
	}
	if l.tx.disposed != 1 {
		t.Fatal("replay reran disposal")
	}
}

type extractionDisposalUnsupportedLedger struct {
	model.Ledger
	tx *receiptCheckTx
}

func (l extractionDisposalUnsupportedLedger) Transact(_ context.Context, _ string, f func(model.Tx) error) error {
	return f(l.tx)
}
func TestExtractionDisposalRequiresAdapterCapability(t *testing.T) {
	s, _, _, c, r := extractionDisposalFixture()
	// A stored receipt remains available without the disposal adapter, but a new
	// command must fail before attempting any retention mutation.
	tx := &receiptCheckTx{receipt: &model.CommitResult{ID: "prior"}}
	s.ledger = extractionDisposalUnsupportedLedger{tx: tx}
	if out, err := s.DisposeExtraction(context.Background(), c, r); err != nil || out.ID != "prior" {
		t.Fatal(out, err)
	}
	// RequireScope must succeed to reach the capability check.
	s.ledger = extractionDisposalNoCapabilityLedger{}
	if _, err := s.DisposeExtraction(context.Background(), c, r); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal(err)
	}
}

type extractionDisposalNoCapabilityTx struct{ receiptCheckTx }

func (*extractionDisposalNoCapabilityTx) RequireScope(context.Context, string) error { return nil }

type extractionDisposalNoCapabilityLedger struct{ model.Ledger }

func (extractionDisposalNoCapabilityLedger) Transact(_ context.Context, _ string, f func(model.Tx) error) error {
	return f(&extractionDisposalNoCapabilityTx{})
}
