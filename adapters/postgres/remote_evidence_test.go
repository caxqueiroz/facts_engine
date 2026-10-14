package postgres_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testRemoteEvidence(t *testing.T, ctx context.Context, engine, metadataOnly *facts.Engine, caller model.Caller, source model.Evidence, redaction model.RedactedEvidenceRequest) {
	t.Helper()
	clientFor := func(engine *facts.Engine) memoryv1connect.EvidenceServiceClient {
		h, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return caller, nil }, connectapi.Limits{Timeout: 5 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(h)
		t.Cleanup(server.Close)
		return memoryv1connect.NewEvidenceServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	}
	client := clientFor(engine)
	object := func(r model.ObjectRef) *v1.ObjectRef {
		return &v1.ObjectRef{Locator: r.Locator, Version: r.Version, Digest: r.Digest, Size: r.Size}
	}
	source.ID = "remote-original"
	source.SourceObject = "remote-message"
	command := model.Command{Key: "remote-original", Purpose: "redaction-test"}
	request := &v1.RegisterEvidenceRequest{Tenant: caller.Tenant, Command: &v1.Command{Key: command.Key, Purpose: command.Purpose}, Evidence: &v1.Evidence{
		Id: source.ID, Scope: source.Scope, SourceSystem: source.SourceSystem, SourceObject: source.SourceObject, SourceRevision: source.SourceRevision, Speaker: source.Speaker, SourceClass: source.SourceClass, MediaType: source.MediaType, Classification: source.Classification, RetentionPolicy: source.RetentionPolicy, SourceTime: timestamppb.New(source.SourceTime), Object: object(source.Object),
	}}
	registered, err := client.RegisterEvidence(ctx, request)
	if err != nil || registered.Result.Id != source.ID {
		t.Fatalf("remote registration: %v, %v", registered, err)
	}
	replay, err := engine.RegisterEvidence(ctx, caller, model.EvidenceRequest{Command: command, Evidence: source})
	if err != nil || replay.ID != registered.Result.Id || replay.LedgerRevision != registered.Result.LedgerRevision {
		t.Fatalf("embedded evidence replay: %+v, %v", replay, err)
	}
	redaction.ID = "remote-redacted"
	redaction.ParentEvidenceID = source.ID
	redaction.Command = model.Command{Key: "remote-redact", Purpose: command.Purpose}
	derived := &v1.RegisterRedactedEvidenceRequest{Tenant: caller.Tenant, Command: &v1.Command{Key: redaction.Key, Purpose: redaction.Purpose}, Id: redaction.ID, Scope: redaction.Scope, ParentEvidenceId: redaction.ParentEvidenceID, PolicyVersion: redaction.PolicyVersion, Object: object(redaction.Object)}
	for _, r := range redaction.Ranges {
		derived.Ranges = append(derived.Ranges, &v1.EvidenceCopyRange{OriginalStart: int64(r.OriginalStart), OriginalEnd: int64(r.OriginalEnd), DerivedStart: int64(r.DerivedStart)})
	}
	result, err := client.RegisterRedactedEvidence(ctx, derived)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := engine.RegisterRedactedEvidence(ctx, caller, redaction)
	if err != nil || repeated.ID != result.Result.Id || repeated.ContentDigest != result.Result.ContentDigest || repeated.LedgerRevision != result.Result.LedgerRevision {
		t.Fatalf("derivative replay: %+v, %v", repeated, err)
	}
	content, err := client.GetEvidenceContent(ctx, &v1.GetEvidenceContentRequest{Tenant: caller.Tenant, Scope: source.Scope, Id: redaction.ID, Purpose: command.Purpose})
	if err != nil {
		t.Fatal(err)
	}
	if string(content.Content) != "I prefer tea." || model.BytesDigest(content.Content) != content.Metadata.Object.Digest || content.Metadata.Derivation.ParentEvidenceId != source.ID || len(content.Metadata.Derivation.Ranges) != 2 || content.Metadata.Derivation.Ranges[1].OriginalStart != 9 || content.Metadata.Classification != source.Classification || content.Metadata.SourceClass != source.SourceClass {
		t.Fatalf("derivative content/provenance: %v", content)
	}
	limited := clientFor(metadataOnly)
	metadata, err := limited.GetEvidenceMetadata(ctx, &v1.GetEvidenceMetadataRequest{Tenant: caller.Tenant, Scope: source.Scope, Id: redaction.ID, Purpose: command.Purpose})
	if err != nil || metadata.Metadata.Object.Digest != content.Metadata.Object.Digest {
		t.Fatalf("metadata-only read: %v, %v", metadata, err)
	}
	if _, err := limited.GetEvidenceContent(ctx, &v1.GetEvidenceContentRequest{Tenant: caller.Tenant, Scope: source.Scope, Id: redaction.ID, Purpose: command.Purpose}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("content permission bypass: %v", err)
	}
	if _, err := client.GetEvidenceMetadata(ctx, &v1.GetEvidenceMetadataRequest{Tenant: caller.Tenant, Scope: "other", Id: redaction.ID, Purpose: command.Purpose}); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("scope mismatch: %v", err)
	}
	request.Command.Key = "remote-spoof"
	request.Evidence = content.Metadata
	if _, err := client.RegisterEvidence(ctx, request); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("forged derivative accepted: %v", err)
	}
}
