package postgres_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/connectproto"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Exercise the same persisted command through both entry points. A remote retry
// must return the original receipt, including its original tenant revision.
func testRemoteFactParity(t *testing.T, ctx context.Context, engine *facts.Engine, caller model.Caller, proposal model.FactProposal, committed model.CommitResult) {
	t.Helper()
	handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return caller, nil },
		connectapi.Limits{Timeout: 5 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := memoryv1connect.NewFactServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	request := &v1.ProposeFactRequest{
		Tenant: caller.Tenant, Command: &v1.Command{Key: proposal.Command.Key, Purpose: proposal.Purpose, CorrelationId: proposal.CorrelationID},
		Key:           &v1.FactKey{Scope: proposal.Key.Scope, Subject: proposal.Key.Subject, Predicate: proposal.Key.Predicate, Discriminator: proposal.Key.Discriminator},
		SchemaVersion: proposal.SchemaVersion, Value: &v1.Value{Payload: &v1.Value_Text{Text: *proposal.Value.Text}},
		Valid: &v1.Interval{From: timestamppb.New(*proposal.Valid.From)}, ExpectedRevision: proposal.ExpectedRevision,
	}
	for _, source := range proposal.Sources {
		request.Sources = append(request.Sources, &v1.SourceSpan{EvidenceId: source.EvidenceID, Start: int64(source.Start), End: int64(source.End), Quote: source.Quote})
	}
	replay, err := client.ProposeFact(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	got := replay.Result
	if got.Id != committed.ID || got.RecordId != committed.RecordID || got.VersionId != committed.VersionID || got.ContentDigest != committed.ContentDigest || got.LedgerRevision != committed.LedgerRevision || got.StreamRevision != committed.StreamRevision {
		t.Fatalf("remote replay changed receipt: %v, %+v", got, committed)
	}
	query := model.FactQuery{Purpose: "integration", Key: proposal.Key, ValidAt: *proposal.Valid.From}
	embedded, err := engine.GetFact(ctx, caller, query)
	if err != nil {
		t.Fatal(err)
	}
	remote, err := client.GetFact(ctx, &v1.GetFactRequest{Tenant: caller.Tenant, Purpose: query.Purpose, Key: request.Key, ValidAt: request.Valid.From})
	if err != nil || remote.Result.State != embedded.State || remote.Result.LedgerRevision != embedded.LedgerRevision || remote.Result.StreamRevision != embedded.StreamRevision || remote.Result.RecordId != embedded.RecordID || remote.Result.VersionId != embedded.VersionID {
		t.Fatalf("remote read differs: %v, %+v, %v", remote, embedded, err)
	}
	request.Command.Key = "remote-revision-conflict"
	request.ExpectedRevision = 999
	_, err = client.ProposeFact(ctx, request)
	var rpc *connect.Error
	if !errors.As(err, &rpc) || rpc.Code() != connect.CodeAborted || len(rpc.Details()) != 1 {
		t.Fatalf("revision error: %T %v; details=%v", err, err, rpc.Details())
	}
	detail, err := connectproto.UnmarshalErrorDetail(rpc.Details()[0])
	if err != nil {
		t.Fatal(err)
	}
	conflict, ok := detail.(*v1.RevisionConflict)
	if !ok || conflict.RecordId != committed.RecordID || conflict.CurrentRevision != committed.StreamRevision || conflict.ExpectedRevision != 999 {
		t.Fatalf("conflict detail: %v", detail)
	}
}

func testRemoteFactReads(t *testing.T, ctx context.Context, engine *facts.Engine, caller model.Caller, key model.FactKey, recordID string, validAt time.Time) {
	t.Helper()
	makeClients := func(identity model.Caller) (memoryv1connect.FactServiceClient, memoryv1connect.SnapshotServiceClient) {
		handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return identity, nil }, connectapi.Limits{Timeout: 5 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		client := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL))
		return memoryv1connect.NewFactServiceClient(client), memoryv1connect.NewSnapshotServiceClient(client)
	}
	client, snapshots := makeClients(caller)
	list := &v1.ListFactsRequest{Tenant: caller.Tenant, Scope: key.Scope, Subject: key.Subject, Purpose: "integration", ValidAt: timestamppb.New(validAt), Limit: 1}
	first, err := client.ListFacts(ctx, list)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 1 || first.NextPageToken == "" || first.Items[0].Resolution.Value.GetText() != "tea" {
		t.Fatalf("first page: %v", first)
	}
	list.PageToken = first.NextPageToken
	second, err := client.ListFacts(ctx, list)
	if err != nil || len(second.Items) != 1 || second.LedgerRevision != first.LedgerRevision || !second.EvaluatedAt.AsTime().Equal(first.EvaluatedAt.AsTime()) || second.Items[0].Selector.Predicate == first.Items[0].Selector.Predicate {
		t.Fatalf("second page: %v, %v", second, err)
	}
	// Signed cursors cannot be reused by another actor or with changed filters.
	other := caller
	other.Principal = "other-reviewer"
	otherClient, _ := makeClients(other)
	if _, err := otherClient.ListFacts(ctx, list); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("cross-principal token: %v", err)
	}
	list.Predicate = "preference"
	if _, err := client.ListFacts(ctx, list); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("changed-filter token: %v", err)
	}
	history := &v1.GetFactHistoryRequest{Tenant: caller.Tenant, Scope: key.Scope, RecordId: recordID, Purpose: "integration", Limit: 1}
	h1, err := client.GetFactHistory(ctx, history)
	if err != nil {
		t.Fatal(err)
	}
	if len(h1.Events) != 1 || h1.NextPageToken == "" || h1.Events[0].StreamRevision != 1 || h1.Events[0].Type != "propose_fact" || h1.Events[0].Caller.Principal != "proposer" || !h1.Events[0].Authorization.Allowed {
		t.Fatalf("first history: %v", h1)
	}
	history.PageToken = h1.NextPageToken
	h2, err := client.GetFactHistory(ctx, history)
	if err != nil || len(h2.Events) != 1 || h2.Events[0].StreamRevision != 2 || h2.Events[0].Type != "approve_fact" || h2.NextPageToken != "" || h2.LedgerRevision != h1.LedgerRevision {
		t.Fatalf("second history: %v, %v", h2, err)
	}
	if _, err := otherClient.GetFactHistory(ctx, history); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("history token actor: %v", err)
	}
	request := &v1.CreateFactSnapshotRequest{Tenant: caller.Tenant, Command: &v1.Command{Key: "remote-snapshot", Purpose: "integration"}, Selectors: []*v1.FactKey{{Scope: key.Scope, Subject: key.Subject, Predicate: key.Predicate}}, ValidAt: timestamppb.New(validAt)}
	created, err := snapshots.CreateFactSnapshot(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	saved := created.Snapshot
	embedded, err := engine.GetFactSnapshot(ctx, caller, model.SnapshotQuery{ID: saved.Id, Purpose: "integration"})
	if err != nil {
		t.Fatal(err)
	}
	if saved.Digest != embedded.Digest || saved.SelectionRevision != embedded.SelectionRevision || saved.CreationRevision != embedded.CreationRevision || saved.CreationRevision <= saved.SelectionRevision || saved.CanonicalVersion != embedded.CanonicalVersion || len(saved.Items) != 1 || saved.Items[0].Resolution.Value.GetText() != "tea" || saved.Items[0].EvidenceIds[0] != "ev1" || saved.Caller.Principal != caller.Principal || len(saved.Authorization) == 0 {
		t.Fatalf("snapshot differs: %v, %+v", saved, embedded)
	}
	fetched, err := snapshots.GetFactSnapshot(ctx, &v1.GetFactSnapshotRequest{Tenant: caller.Tenant, Id: saved.Id, Purpose: "integration"})
	if err != nil || !proto.Equal(saved, fetched.Snapshot) {
		t.Fatalf("get snapshot differs: %v, %v", fetched, err)
	}
	// Retry through the embedded entry point must return the remote commit.
	retry, err := engine.CreateFactSnapshot(ctx, caller, model.FactSnapshotRequest{Command: model.Command{Key: "remote-snapshot", Purpose: "integration"}, Selectors: []model.FactKey{key}, ValidAt: validAt})
	if err != nil || retry.ID != saved.Id || retry.Digest != saved.Digest {
		t.Fatalf("embedded snapshot retry differs: %+v, %v", retry, err)
	}
}
