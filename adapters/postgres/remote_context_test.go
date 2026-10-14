package postgres_test

import (
	"bytes"
	"connectrpc.com/connect/v2"
	"context"
	facts "github.com/caxqueiroz/facts_engine"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
)

func contextRequestProto(caller model.Caller, r model.ContextRequest) *v1.PrepareContextRequest {
	q := r.Query
	return &v1.PrepareContextRequest{Tenant: caller.Tenant, Context: &v1.ContextInput{Command: &v1.Command{Key: r.Key, Purpose: r.Purpose, CorrelationId: r.CorrelationID}, Scope: r.Scope, IncludeMemories: r.IncludeMemories, Query: &v1.RecallQuery{Scopes: q.Scopes, Query: q.Query, Purpose: q.Purpose, ValidAt: timestamppb.New(q.ValidAt), KnowledgeRevision: q.KnowledgeRevision, RequireRevision: q.RequireRevision, RequireGeneration: q.RequireGeneration, Limit: int32(q.Limit)}}}
}
func remotePrepareContext(t *testing.T, ctx context.Context, e *facts.Engine, caller model.Caller, r model.ContextRequest) (model.ContextManifest, error) {
	t.Helper()
	client := memoryv1connect.NewContextServiceClient(governanceClient(t, e, caller))
	request := contextRequestProto(caller, r)
	created, err := client.PrepareContext(ctx, request)
	if err != nil {
		return model.ContextManifest{}, err
	}
	direct, err := e.PrepareContext(ctx, caller, r)
	if err != nil {
		t.Fatal(err)
	}
	m := created.Manifest
	if m.Id != direct.ID || m.Digest != direct.Digest || m.RenderedDigest != model.BytesDigest(m.Rendered) || !bytes.Equal(m.Rendered, direct.Rendered) || m.SelectionRevision != direct.SelectionRevision || m.IndexedRevision != direct.IndexedRevision || m.CreationRevision != direct.CreationRevision || m.Incomplete != direct.Incomplete || m.CanonicalVersion != direct.CanonicalVersion || m.RedactionVersion != direct.RedactionVersion || m.QueryReference != direct.QueryReference || !proto.Equal(m.Request, request.Context) {
		t.Fatal("context provenance/retry mismatch")
	}
	if m.Retrieval.Generation != direct.Retrieval.Generation || m.Retrieval.EmbeddingId != direct.Retrieval.EmbeddingID || m.Retrieval.SearchVersion != direct.Retrieval.SearchVersion || m.Retrieval.RerankerId != direct.Retrieval.RerankerID || m.Retrieval.Dimensions != int64(direct.Retrieval.Dimensions) || len(m.Authorization) != len(direct.Authorization) || len(m.Items) != len(direct.Items) || len(m.Memories) != len(direct.Memories) || len(m.Order) != len(direct.Order) {
		t.Fatal("context metadata mismatch")
	}
	for i, ref := range m.Order {
		if ref.RecordId != direct.Order[i].RecordID || ref.VersionId != direct.Order[i].VersionID {
			t.Fatal("context order mismatch")
		}
	}
	for i, item := range m.Items {
		if item.Fact.VersionId != direct.Items[i].Fact.VersionID || item.Score != direct.Items[i].Score {
			t.Fatal("context fact mismatch")
		}
	}
	for i, item := range m.Memories {
		if item.Memory.VersionId != direct.Memories[i].Memory.VersionID || item.Memory.Body != direct.Memories[i].Memory.Body || item.Score != direct.Memories[i].Score {
			t.Fatal("context memory mismatch")
		}
	}
	fetched, err := client.GetContextManifest(ctx, &v1.GetContextManifestRequest{Tenant: caller.Tenant, Id: m.Id, Purpose: r.Purpose})
	if err != nil || !proto.Equal(m, fetched.Manifest) {
		t.Fatalf("context read: %v", err)
	}
	return direct, nil
}
func remoteRecordDecision(t *testing.T, ctx context.Context, e *facts.Engine, caller model.Caller, r model.DecisionReceipt) (model.DecisionRecord, error) {
	t.Helper()
	artifact := func(a *model.ArtifactReference) *v1.ArtifactReference {
		if a == nil {
			return nil
		}
		return &v1.ArtifactReference{EvidenceId: a.EvidenceID, Digest: a.Digest}
	}
	refs := func(in []model.VersionReference) []*v1.VersionReference {
		var out []*v1.VersionReference
		for _, v := range in {
			out = append(out, &v1.VersionReference{RecordId: v.RecordID, VersionId: v.VersionID})
		}
		return out
	}
	receipt := &v1.DecisionReceipt{Command: &v1.Command{Key: r.Key, Purpose: r.Purpose, CorrelationId: r.CorrelationID}, Scope: r.Scope, ApplicationId: r.ApplicationID, ExternalDecisionId: r.ExternalDecisionID, ContextId: r.ContextID, SnapshotId: r.SnapshotID, CalculationId: r.CalculationID, SelectedItems: refs(r.SelectedItems), UsedItems: refs(r.UsedItems), MethodKind: r.MethodKind, MethodId: r.MethodID, MethodVersion: r.MethodVersion, TransformationVersion: r.TransformationVersion, Input: artifact(&r.Input), Output: artifact(&r.Output), ModelRequest: artifact(r.ModelRequest), ModelResponse: artifact(r.ModelResponse), ActionReceipt: artifact(r.ActionReceipt), DecidedAt: timestamppb.New(r.DecidedAt), Outcome: r.Outcome}
	client := memoryv1connect.NewContextServiceClient(governanceClient(t, e, caller))
	created, err := client.RecordDecision(ctx, &v1.RecordDecisionRequest{Tenant: caller.Tenant, Receipt: receipt})
	if err != nil {
		return model.DecisionRecord{}, err
	}
	direct, err := e.RecordDecision(ctx, caller, r)
	if err != nil {
		t.Fatal(err)
	}
	record := created.Record
	if record.Id != direct.ID || record.Digest != direct.Digest || record.SourceDigest != direct.SourceDigest || record.CalculationDigest != direct.CalculationDigest || record.LedgerRevision != direct.LedgerRevision || record.Caller.Principal != caller.Principal || !proto.Equal(record.Receipt, receipt) || len(record.Authorization) != len(direct.Authorization) {
		t.Fatal("decision provenance/retry mismatch")
	}
	fetched, err := client.GetDecisionReceipt(ctx, &v1.GetDecisionReceiptRequest{Tenant: caller.Tenant, Id: record.Id, Purpose: r.Purpose})
	if err != nil || !proto.Equal(record, fetched.Record) {
		t.Fatalf("decision read: %v", err)
	}
	receipt.Command.Key += "-external-retry"
	again, err := client.RecordDecision(ctx, &v1.RecordDecisionRequest{Tenant: caller.Tenant, Receipt: receipt})
	if err != nil || again.Record.Id != record.Id {
		t.Fatalf("decision external identity: %v", err)
	}
	receipt.Command.Key += "-changed"
	receipt.Outcome = "failed"
	if r.Outcome == "failed" {
		receipt.Outcome = "succeeded"
	}
	if _, err := client.RecordDecision(ctx, &v1.RecordDecisionRequest{Tenant: caller.Tenant, Receipt: receipt}); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("decision conflict: %v", err)
	}
	return direct, nil
}
func testRemoteContextFailure(t *testing.T, ctx context.Context, e *facts.Engine, caller model.Caller, r model.ContextRequest) {
	t.Helper()
	client := memoryv1connect.NewContextServiceClient(governanceClient(t, e, caller))
	result, err := client.PrepareContext(ctx, contextRequestProto(caller, r))
	if result != nil || connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("uncommitted context delivered: %v, %v", result, err)
	}
}
func testRemoteContextDenied(t *testing.T, ctx context.Context, e *facts.Engine, caller model.Caller, manifest, decision string) {
	t.Helper()
	client := memoryv1connect.NewContextServiceClient(governanceClient(t, e, caller))
	if _, err := client.GetContextManifest(ctx, &v1.GetContextManifestRequest{Tenant: caller.Tenant, Id: manifest, Purpose: "review"}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("context permission: %v", err)
	}
	if _, err := client.GetDecisionReceipt(ctx, &v1.GetDecisionReceiptRequest{Tenant: caller.Tenant, Id: decision, Purpose: "review"}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("decision permission: %v", err)
	}
}
