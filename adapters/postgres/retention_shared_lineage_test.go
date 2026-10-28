package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
)

func testRetentionSharedLineage(t *testing.T, ctx context.Context, ledger *postgres.Ledger, store *evidence.Postgres, engine *facts.Engine, c model.Caller, register func(string, string, string, string) model.ObjectRef) {
	t.Helper()
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "test-disposal"} }
	redactor, err := facts.NewWithOptions(ledger, allow{}, store, model.EngineOptions{RedactionPolicyVersions: []string{"shared-redaction-a", "shared-redaction-b"}})
	if err != nil {
		t.Fatal(err)
	}
	family := func(prefix string) (model.ObjectRef, model.EvidenceDisposalRequest) {
		t.Helper()
		object := prefix + "-object"
		root := prefix + "-root"
		ref := register(root, object, "case", "short-lived")
		children := []string{prefix + "-a", prefix + "-b"}
		for i, id := range children {
			policy := []string{"shared-redaction-a", "shared-redaction-b"}[i]
			result, err := redactor.RegisterRedactedEvidence(ctx, c, model.RedactedEvidenceRequest{Command: cmd(id), ID: id, Scope: "case", ParentEvidenceID: root, PolicyVersion: policy, Object: ref, Ranges: []model.EvidenceCopyRange{{OriginalEnd: len(object)}}})
			if err != nil || result.ID != id {
				t.Fatal("shared derivative registration", result, err)
			}
		}
		return ref, model.EvidenceDisposalRequest{Command: cmd(prefix + "-dispose"), Scope: "case", EvidenceID: root, PolicyID: "short-lived", Jurisdiction: "XX", Reason: "expired", DerivativeEvidenceIDs: children}
	}
	ref, request := family("shared-lineage")
	missing := request
	missing.DerivativeEvidenceIDs = request.DerivativeEvidenceIDs[:1]
	if _, err := engine.DisposeEvidence(ctx, c, missing); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("incomplete shared family disposed", err)
	}
	if _, err := store.Read(ctx, c.Tenant, ref); err != nil {
		t.Fatal(err)
	}
	failing, err := facts.New(rollbackRetentionLedger{ledger}, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.DisposeEvidence(ctx, c, request); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("shared disposal rollback", err)
	}
	if _, err := store.Read(ctx, c.Tenant, ref); err != nil {
		t.Fatal("shared object deletion escaped rollback", err)
	}
	first, err := engine.DisposeEvidence(ctx, c, request)
	if err != nil {
		t.Fatal("complete shared family rejected", err)
	}
	again, err := engine.DisposeEvidence(ctx, c, request)
	if err != nil || first != again {
		t.Fatal("shared disposal replay", err)
	}
	if _, err := store.Read(ctx, c.Tenant, ref); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("shared bytes survived", err)
	}
	for _, id := range append([]string{request.EvidenceID}, request.DerivativeEvidenceIDs...) {
		status, err := engine.GetRetentionStatus(ctx, c, model.RetentionStatusQuery{Scope: "case", EvidenceID: id, Purpose: request.Purpose})
		if err != nil || !status.Disposed {
			t.Fatal("missing shared tombstone", id, err)
		}
	}

	// An alias registered in another scope changes the shared object epoch, even
	// though that scope does not hold the lineage's scope lock.
	protected, raceRequest := family("shared-race")
	ready, resume := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	var once sync.Once
	go func() {
		finished <- ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
			if _, err := tx.(model.RetentionTx).RetentionStatus(ctx, "case", raceRequest.EvidenceID); err != nil {
				return err
			}
			once.Do(func() { close(ready); <-resume })
			return tx.(model.EvidenceLineageDisposalTx).DisposeEvidenceLineage(ctx, raceRequest)
		})
	}()
	<-ready
	// Avoid Fatal while the disposal goroutine is waiting.
	source, sourceErr := engine.GetEvidenceMetadata(ctx, c, model.EvidenceQuery{ID: raceRequest.EvidenceID, Scope: "case", Purpose: raceRequest.Purpose})
	var aliasErr error
	if sourceErr == nil {
		source.ID = "shared-outside-alias"
		source.Scope = "remote-retention"
		source.SourceObject = "shared-outside-alias"
		source.RetentionPolicy = "remote-policy"
		_, aliasErr = engine.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: cmd("shared-outside-alias"), Evidence: source})
	}
	close(resume)
	raceErr := <-finished
	if sourceErr != nil || aliasErr != nil {
		t.Fatal(sourceErr, aliasErr)
	}
	if !errors.Is(raceErr, model.ErrPrecondition) {
		t.Fatal("unselected concurrent alias bypassed", raceErr)
	}
	if _, err := store.Read(ctx, c.Tenant, protected); err != nil {
		t.Fatal("outside alias lost content", err)
	}
	if _, err := engine.DisposeEvidence(ctx, c, raceRequest); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("outside alias ignored", err)
	}
	raceRequest.DerivativeEvidenceIDs = append(append([]string(nil), raceRequest.DerivativeEvidenceIDs...), "shared-outside-alias")
	if _, err := engine.DisposeEvidence(ctx, c, raceRequest); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross-scope alias admitted", err)
	}
	if _, err := store.Read(ctx, c.Tenant, protected); err != nil {
		t.Fatal(err)
	}
}
