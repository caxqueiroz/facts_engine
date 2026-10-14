package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/model"
)

func testMemoryAttestations(t *testing.T, ctx context.Context, engine *facts.Engine, c, reviewer, service model.Caller, request model.MemoryProposal, version model.CommitResult, approval model.Approval) {
	t.Helper()
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "memory-validation"} }
	att := model.AttestationRequest{Command: cmd("memory-confirm"), Attestation: model.Attestation{ID: "memory-confirmation", Scope: request.Key.Scope, RecordID: version.RecordID, VersionID: version.VersionID, ContentDigest: version.ContentDigest, Kind: "application_validation", EvidenceID: request.Sources[0].EvidenceID, Attestor: c}}
	wrong := att
	wrong.Attestation.Attestor = reviewer
	if _, err := engine.RecordAttestation(ctx, c, wrong); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("forged memory attestor", err)
	}
	wrong = att
	wrong.Attestation.ContentDigest = model.BytesDigest([]byte("wrong"))
	if _, err := engine.RecordAttestation(ctx, c, wrong); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("wrong memory digest", err)
	}
	wrong = att
	past := time.Now().Add(-time.Second)
	wrong.Attestation.ExpiresAt = &past
	if _, err := engine.RecordAttestation(ctx, c, wrong); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("expired attestation", err)
	}
	// att1 already identifies a fact attestation in this tenant's integration fixture.
	wrong = att
	wrong.Command = cmd("memory-kind-collision")
	wrong.Attestation.ID = "att1"
	if _, err := engine.RecordAttestation(ctx, c, wrong); !errors.Is(err, model.ErrConflict) {
		t.Fatal("cross-kind attestation identity collision", err)
	}
	recorded, err := engine.RecordAttestation(ctx, c, att)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := engine.RecordAttestation(ctx, c, att)
	if err != nil || retried != recorded {
		t.Fatal("attestation retry", retried, err)
	}
	accepted, err := engine.AcceptMemory(ctx, service, approval)
	if err != nil {
		t.Fatal("valid prerequisites rejected", err)
	}
	query := model.MemoryQuery{Key: request.Key, Purpose: "memory-validation", ValidAt: *request.Valid.From}
	current, err := engine.GetMemory(ctx, reviewer, query)
	if err != nil || current.State != "resolved" {
		t.Fatal(current, err)
	}
	unused := att
	unused.Command = cmd("memory-unused-confirm")
	unused.Attestation.ID = "memory-confirmation-unused"
	if _, err = engine.RecordAttestation(ctx, c, unused); err != nil {
		t.Fatal(err)
	}
	withdrawal := model.Withdrawal{Command: cmd("memory-withdraw"), Scope: request.Key.Scope, AttestationID: att.Attestation.ID}
	if _, err = engine.WithdrawAttestation(ctx, reviewer, withdrawal); !errors.Is(err, model.ErrDenied) {
		t.Fatal("another actor withdrew memory attestation", err)
	}
	withdrawn, err := engine.WithdrawAttestation(ctx, c, withdrawal)
	if err != nil || withdrawn.RecordKind != "memory" {
		t.Fatal(withdrawn, err)
	}
	again, err := engine.WithdrawAttestation(ctx, c, withdrawal)
	if err != nil || again != withdrawn {
		t.Fatal("withdrawal retry", again, err)
	}
	current, err = engine.GetMemory(ctx, reviewer, query)
	if err != nil || current.State != "ineligible" || current.Body != "" {
		t.Fatal("unused attestation resurrected accepted memory", current, err)
	}
	historical := query
	historical.KnowledgeRevision = accepted.LedgerRevision
	previous, err := engine.GetMemory(ctx, reviewer, historical)
	if err != nil || previous.State != "resolved" {
		t.Fatal("withdrawal rewrote historical eligibility", previous, err)
	}
	corrected := request
	corrected.Command = cmd("memory-attested-correction")
	corrected.ExpectedRevision = withdrawn.StreamRevision
	corrected.Body = "Corrected memory requiring its own validation."
	next, err := engine.ProposeMemory(ctx, c, corrected)
	if err != nil {
		t.Fatal(err)
	}
	stale := att
	stale.Command = cmd("memory-stale-confirm")
	stale.Attestation.ID = "memory-correction-confirmation"
	stale.Attestation.VersionID = next.VersionID
	if _, err = engine.RecordAttestation(ctx, c, stale); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("old digest confirmed new version", err)
	}
	stale.Attestation.ContentDigest = next.ContentDigest
	if _, err = engine.RecordAttestation(ctx, c, stale); err != nil {
		t.Fatal(err)
	}
	replacement := model.Supersession{Approval: model.Approval{Command: cmd("memory-attested-supersede"), Scope: request.Key.Scope, RecordID: version.RecordID, VersionID: next.VersionID, ExpectedRevision: next.StreamRevision}, Valid: request.Valid, ReplacedVersionIDs: []string{version.VersionID}, Reason: "validated_correction", Automated: true}
	if _, err = engine.SupersedeMemory(ctx, service, replacement); err != nil {
		t.Fatal(err)
	}
	current, err = engine.GetMemory(ctx, reviewer, query)
	if err != nil || current.State != "resolved" || current.VersionID != next.VersionID {
		t.Fatal(current, err)
	}
	history, err := engine.GetMemoryHistory(ctx, reviewer, model.HistoryPageQuery{HistoryQuery: model.HistoryQuery{Scope: request.Key.Scope, RecordID: version.RecordID, Purpose: "memory-history"}, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	withdrawalFound := false
	for _, event := range history.Events {
		if event.Type == "withdraw_attestation" && event.EntityID == att.Attestation.ID {
			withdrawalFound = true
		}
	}
	if !withdrawalFound {
		t.Fatal("memory history lost attestation withdrawal")
	}

}
