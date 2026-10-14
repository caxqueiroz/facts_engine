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
