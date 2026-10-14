package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	facts "github.com/caxqueiroz/facts_engine"
	audits "github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testRemoteCalculation(t *testing.T, ctx context.Context, engine *facts.Engine, caller model.Caller, r model.CalculationReceipt) {
	t.Helper()
	client := memoryv1connect.NewCalculationServiceClient(governanceClient(t, engine, caller))
	r.Key = "remote-calculation"
	r.ExternalRunID = "remote-run"
	request := &v1.RecordCalculationRequest{Tenant: caller.Tenant, Receipt: &v1.CalculationReceipt{Command: &v1.Command{Key: r.Key, Purpose: r.Purpose, CorrelationId: r.CorrelationID}, Scope: r.Scope, SnapshotId: r.SnapshotID, ApplicationId: r.ApplicationID, ExternalRunId: r.ExternalRunID, MethodId: r.MethodID, MethodVersion: r.MethodVersion, ExecutableDigest: r.ExecutableDigest, ConfigurationDigest: r.ConfigurationDigest, NormalizationVersion: r.NormalizationVersion, ReferenceSnapshotIds: r.ReferenceSnapshotIDs, Input: &v1.ArtifactReference{EvidenceId: r.Input.EvidenceID, Digest: r.Input.Digest}, Output: &v1.ArtifactReference{EvidenceId: r.Output.EvidenceID, Digest: r.Output.Digest}, StartedAt: timestamppb.New(r.StartedAt), CompletedAt: timestamppb.New(r.CompletedAt), Outcome: r.Outcome}}
	created, err := client.RecordCalculation(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	embedded, err := engine.RecordCalculation(ctx, caller, r)
	if err != nil || embedded.ID != created.Record.Id || embedded.Digest != created.Record.Digest || embedded.SnapshotDigest != created.Record.SnapshotDigest || embedded.LedgerRevision != created.Record.LedgerRevision {
		t.Fatalf("calculation parity: %+v, %v", embedded, err)
	}
	if !proto.Equal(created.Record.Receipt, request.Receipt) || created.Record.Caller.Principal != caller.Principal || len(created.Record.Authorization) == 0 {
		t.Fatalf("calculation provenance: %v", created)
	}
	fetched, err := client.GetCalculationReceipt(ctx, &v1.GetCalculationReceiptRequest{Tenant: caller.Tenant, Id: embedded.ID, Purpose: r.Purpose})
	if err != nil || !proto.Equal(created.Record, fetched.Record) {
		t.Fatalf("calculation read: %v, %v", fetched, err)
	}
	request.Receipt.Command.Key = "remote-same-external-run"
	duplicate, err := client.RecordCalculation(ctx, request)
	if err != nil || duplicate.Record.Id != embedded.ID {
		t.Fatalf("external run idempotency: %v, %v", duplicate, err)
	}
	request.Receipt.Command.Key = "remote-changed-run"
	request.Receipt.ConfigurationDigest = model.BytesDigest([]byte("changed"))
	if _, err := client.RecordCalculation(ctx, request); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("external run conflict: %v", err)
	}
	request.Receipt.ExternalRunId = "remote-bad-artifact"
	request.Receipt.Command.Key = "remote-bad-artifact"
	request.Receipt.Input.Digest = model.BytesDigest([]byte("wrong"))
	if _, err := client.RecordCalculation(ctx, request); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("artifact digest bypass: %v", err)
	}
}

func testRemoteAudit(t *testing.T, ctx context.Context, engine, restricted *facts.Engine, caller model.Caller, public ed25519.PublicKey) {
	t.Helper()
	makeClient := func(e *facts.Engine) memoryv1connect.AuditServiceClient {
		h, err := connectapi.NewHandler(e, func(context.Context) (model.Caller, error) { return caller, nil }, connectapi.Limits{Timeout: 10 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 128 << 20})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(h)
		t.Cleanup(server.Close)
		return memoryv1connect.NewAuditServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL, connecthttp.WithReadMaxBytes(128<<20))))
	}
	client := makeClient(engine)
	request := &v1.StartAuditExportRequest{Tenant: caller.Tenant, Command: &v1.Command{Key: "remote-audit", Purpose: "audit"}, Scope: "case1", IncludeEvidence: true}
	pending, err := client.StartAuditExport(ctx, request)
	if err != nil || pending.Result.State != "pending" || pending.Result.Bundle != nil || pending.Result.Artifact != nil {
		t.Fatalf("pending audit: %v, %v", pending, err)
	}
	duplicate, err := engine.StartAuditExport(ctx, caller, model.AuditExportRequest{Command: model.Command{Key: "remote-audit", Purpose: "audit"}, Scope: "case1", IncludeEvidence: true})
	if err != nil || duplicate.ID != pending.Result.Id {
		t.Fatalf("audit start retry: %+v, %v", duplicate, err)
	}
	completed, err := engine.RunAuditExport(ctx, caller, model.AuditExportQuery{ID: pending.Result.Id, Scope: "case1", Purpose: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	query := &v1.GetAuditExportRequest{Tenant: caller.Tenant, Scope: "case1", Id: completed.ID, Purpose: "audit"}
	fetched, err := client.GetAuditExport(ctx, query)
	if err != nil || fetched.Result.State != "completed" || fetched.Result.CompletedRevision != completed.CompletedRevision {
		t.Fatalf("completed audit: %v, %v", fetched, err)
	}
	bundle := fetched.Result.Bundle
	canonical, err := json.Marshal(completed.Bundle.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, bundle.ManifestJson) || !ed25519.Verify(public, bundle.ManifestJson, bundle.Signature.Signature) {
		t.Fatal("signed manifest bytes changed in transport")
	}
	var manifest model.AuditManifest
	if err := json.Unmarshal(bundle.ManifestJson, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.ExportID != bundle.Manifest.ExportId || manifest.Tenant != bundle.Manifest.Tenant || manifest.Scope != bundle.Manifest.Scope || manifest.ThroughRevision != bundle.Manifest.ThroughRevision || int64(manifest.EntryCount) != bundle.Manifest.EntryCount || len(manifest.Entries) != len(bundle.Entries) || len(manifest.Entries) != len(bundle.Manifest.Entries) {
		t.Fatal("typed manifest differs from signed metadata")
	}
	verified := model.AuditBundle{Manifest: manifest, Signature: model.AuditSignature{Algorithm: bundle.Signature.Algorithm, KeyID: bundle.Signature.KeyId, Bytes: bundle.Signature.Signature}}
	for i, entry := range bundle.Entries {
		declared := bundle.Manifest.Entries[i]
		if declared.Kind != manifest.Entries[i].Kind || declared.Id != manifest.Entries[i].ID || declared.Digest != manifest.Entries[i].Digest || model.BytesDigest(entry.PayloadJson) != declared.Digest {
			t.Fatal("entry digest changed in transport")
		}
		verified.Entries = append(verified.Entries, model.AuditEntry{Kind: entry.Kind, ID: entry.Id, Payload: entry.PayloadJson})
	}
	if err := audits.Verify(verified, map[string]ed25519.PublicKey{"test-signing-key": public}); err != nil {
		t.Fatal(err)
	}
	artifact, err := json.Marshal(verified)
	if err != nil || model.BytesDigest(artifact) != fetched.Result.Artifact.Digest || int64(len(artifact)) != fetched.Result.Artifact.Size {
		t.Fatalf("canonical artifact changed: %v", err)
	}
	verified.Entries[0].Payload = []byte("{}")
	if audits.Verify(verified, map[string]ed25519.PublicKey{"test-signing-key": public}) == nil {
		t.Fatal("tampered downloaded entry accepted")
	}
	if _, err := makeClient(restricted).GetAuditExport(ctx, query); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("evidence permission bypass: %v", err)
	}
	query.Scope = "case2"
	if _, err := client.GetAuditExport(ctx, query); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("audit scope mismatch: %v", err)
	}
	changes := &v1.ListChangesRequest{Tenant: caller.Tenant, Scope: "case1", Purpose: "audit", Limit: 2}
	first, err := client.ListChanges(ctx, changes)
	if err != nil || len(first.Events) != 2 || first.NextRevision <= 0 || first.NextRevision > first.ThroughRevision {
		t.Fatalf("change cursor: %v, %v", first, err)
	}
	direct, err := engine.ListChanges(ctx, caller, model.ChangesQuery{Scope: "case1", Purpose: "audit", Limit: 2})
	if err != nil || direct.Events[0].ID != first.Events[0].Id || direct.Events[1].ID != first.Events[1].Id || direct.NextRevision != first.NextRevision {
		t.Fatalf("change parity: %+v, %v", direct, err)
	}
	changes.AfterRevision = first.NextRevision
	second, err := client.ListChanges(ctx, changes)
	if err != nil || len(second.Events) != 2 || second.Events[0].LedgerRevision <= first.NextRevision {
		t.Fatalf("change continuation: %v, %v", second, err)
	}
	changes.AfterRevision = second.ThroughRevision + 1
	if _, err := client.ListChanges(ctx, changes); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("future cursor accepted: %v", err)
	}
}
