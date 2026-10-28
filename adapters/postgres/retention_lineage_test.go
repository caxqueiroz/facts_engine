package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func testRetentionLineage(t *testing.T, ctx context.Context, ledger *postgres.Ledger, store *evidence.Postgres, engine *facts.Engine, c model.Caller, register func(string, string, string, string) model.ObjectRef) {
	t.Helper()
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "test-disposal"} }
	redactor, err := facts.NewWithOptions(ledger, allow{}, store, model.EngineOptions{RedactionPolicyVersions: []string{"lineage-v1"}})
	if err != nil {
		t.Fatal(err)
	}
	root := register("lineage-root", "lineage-original-bytes", "case", "short-lived")
	refs := []model.ObjectRef{root}
	ids := []string{"lineage-a", "lineage-b"}
	for i, id := range ids {
		length := 4 + i*4
		ref, err := store.Put(ctx, c.Tenant, id, []byte("lineage-original-bytes")[:length])
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
		_, err = redactor.RegisterRedactedEvidence(ctx, c, model.RedactedEvidenceRequest{Command: cmd(id), ID: id, Scope: "case", ParentEvidenceID: "lineage-root", PolicyVersion: "lineage-v1", Object: ref, Ranges: []model.EvidenceCopyRange{{OriginalEnd: length}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	request := model.EvidenceDisposalRequest{Command: cmd("lineage-dispose"), Scope: "case", EvidenceID: "lineage-root", PolicyID: "short-lived", Jurisdiction: "XX", Reason: "expired", DerivativeEvidenceIDs: ids}
	intact := func() {
		t.Helper()
		for _, ref := range refs {
			if _, err := store.Read(ctx, c.Tenant, ref); err != nil {
				t.Fatal("partial disposal", err)
			}
		}
	}
	for _, selected := range [][]string{nil, {ids[0]}, {ids[0], ids[0]}, {ids[0], "lineage-root"}, {ids[0], "hold-race"}} {
		bad := request
		bad.DerivativeEvidenceIDs = selected
		if _, err := engine.DisposeEvidence(ctx, c, bad); err == nil {
			t.Fatal("invalid selection accepted", selected)
		}
		intact()
	}
	crossScope := request
	crossScope.Scope = "remote-retention"
	crossScope.DerivativeEvidenceIDs = ids[:1]
	if _, err := engine.DisposeEvidence(ctx, c, crossScope); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross-scope lineage disclosed", err)
	}
	intact()
	if _, err := engine.PlaceLegalHold(ctx, c, model.LegalHoldRequest{Command: cmd("lineage-hold"), ID: "lineage-hold", Scope: "case", Reason: "review"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.DisposeEvidence(ctx, c, request); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("hold bypassed", err)
	}
	intact()
	if _, err := engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: cmd("lineage-release"), ID: "lineage-hold", Scope: "case", Reason: "closed"}); err != nil {
		t.Fatal(err)
	}

	// A derivative published after a disposal snapshot must not be omitted.
	ready, resume := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	var once sync.Once
	go func() {
		finished <- ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
			if _, err := tx.(model.RetentionTx).RetentionStatus(ctx, "case", request.EvidenceID); err != nil {
				return err
			}
			once.Do(func() { close(ready); <-resume })
			return tx.(model.EvidenceLineageDisposalTx).DisposeEvidenceLineage(ctx, request)
		})
	}()
	<-ready
	extra, putErr := store.Put(ctx, c.Tenant, "lineage-c", []byte("lineage-original-bytes")[:12])
	var registrationErr error
	if putErr == nil {
		_, registrationErr = redactor.RegisterRedactedEvidence(ctx, c, model.RedactedEvidenceRequest{Command: cmd("lineage-c"), ID: "lineage-c", Scope: "case", ParentEvidenceID: request.EvidenceID, PolicyVersion: "lineage-v1", Object: extra, Ranges: []model.EvidenceCopyRange{{OriginalEnd: 12}}})
	}
	close(resume)
	raceErr := <-finished
	if putErr != nil || registrationErr != nil {
		t.Fatal(putErr, registrationErr)
	}
	if !errors.Is(raceErr, model.ErrPrecondition) {
		t.Fatal("concurrent derivative escaped selection", raceErr)
	}
	ids = append(ids, "lineage-c")
	refs = append(refs, extra)
	request.DerivativeEvidenceIDs = ids
	intact()
	failing, err := facts.New(rollbackRetentionLedger{ledger}, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failing.DisposeEvidence(ctx, c, request); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("expected append rollback", err)
	}
	intact()
	handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return c, nil }, connectapi.Limits{Timeout: 10 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := memoryv1connect.NewRetentionServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	grpcClient := v1.NewRetentionServiceClient(governanceGRPC(t, engine, c))
	rpcRequest := &v1.DisposeEvidenceRequest{Tenant: c.Tenant, Command: &v1.Command{Key: request.Key, Purpose: request.Purpose}, Scope: request.Scope, EvidenceId: request.EvidenceID, PolicyId: request.PolicyID, Jurisdiction: request.Jurisdiction, Reason: request.Reason, DerivativeEvidenceIds: ids}
	remote, err := grpcClient.DisposeEvidence(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}
	connectReplay, err := client.DisposeEvidence(ctx, rpcRequest)
	if err != nil || !proto.Equal(remote, connectReplay) {
		t.Fatal("lineage disposal Connect/gRPC replay differs", connectReplay, err)
	}
	replay, err := engine.DisposeEvidence(ctx, c, request)
	if err != nil || replay.ID != remote.Result.Id {
		t.Fatal("cross-transport replay", err)
	}
	changed := request
	changed.DerivativeEvidenceIDs = []string{ids[0]}
	if _, err := engine.DisposeEvidence(ctx, c, changed); !errors.Is(err, model.ErrConflict) {
		t.Fatal("changed selection replay", err)
	}
	rpcRequest.DerivativeEvidenceIds = changed.DerivativeEvidenceIDs
	if result, err := grpcClient.DisposeEvidence(ctx, rpcRequest); result != nil || grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatal("gRPC changed lineage selection replay", result, err)
	}
	for i, id := range append([]string{request.EvidenceID}, ids...) {
		if _, err := store.Read(ctx, c.Tenant, refs[i]); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("bytes survived", id, err)
		}
		status, err := engine.GetRetentionStatus(ctx, c, model.RetentionStatusQuery{Scope: "case", EvidenceID: id, Purpose: request.Purpose})
		if err != nil || !status.Disposed {
			t.Fatal("missing tombstone", id, err)
		}
	}
	page, err := engine.ListChanges(ctx, c, model.ChangesQuery{Scope: "case", Purpose: request.Purpose, AfterRevision: replay.LedgerRevision - 1, Limit: 1})
	if err != nil || len(page.Events) != 1 {
		t.Fatal("disposal event", err)
	}
	archived, err := ledger.ArchivePayload(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant}, Event: page.Events[0]})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range archived.Entries {
		if entry.Kind != "retention" {
			continue
		}
		var event model.RetentionEvent
		if err := json.Unmarshal(entry.Payload, &event); err != nil {
			t.Fatal(err)
		}
		if event.Disposal != nil && slices.Equal(event.Disposal.DerivativeEvidenceIDs, ids) {
			found = true
		}
	}
	if !found {
		t.Fatal("archive lost disposal membership")
	}
	for _, id := range append([]string{request.EvidenceID}, ids...) {
		if !slices.ContainsFunc(archived.Entries, func(entry model.AuditEntry) bool { return entry.Kind == "evidence" && entry.ID == id }) {
			t.Fatal("archive lost disposed evidence metadata", id)
		}
	}

	// A previous single-object derivative disposal must not strand the parent.
	priorRoot := register("lineage-prior-root", "lineage-prior-original", "case", "short-lived")
	priorChild, err := store.Put(ctx, c.Tenant, "lineage-prior-child", []byte("line"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = redactor.RegisterRedactedEvidence(ctx, c, model.RedactedEvidenceRequest{Command: cmd("lineage-prior-child"), ID: "lineage-prior-child", Scope: "case", ParentEvidenceID: "lineage-prior-root", PolicyVersion: "lineage-v1", Object: priorChild, Ranges: []model.EvidenceCopyRange{{OriginalEnd: 4}}}); err != nil {
		t.Fatal(err)
	}
	prior := request
	prior.Command = cmd("lineage-prior-child-dispose")
	prior.EvidenceID = "lineage-prior-child"
	prior.DerivativeEvidenceIDs = nil
	if _, err = engine.DisposeEvidence(ctx, c, prior); err != nil {
		t.Fatal(err)
	}
	prior.Command = cmd("lineage-prior-root-dispose")
	prior.EvidenceID = "lineage-prior-root"
	prior.DerivativeEvidenceIDs = []string{"lineage-prior-child"}
	if _, err = engine.DisposeEvidence(ctx, c, prior); err != nil {
		t.Fatal("disposed child stranded parent", err)
	}
	if _, err = store.Read(ctx, c.Tenant, priorRoot); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal(err)
	}

}
