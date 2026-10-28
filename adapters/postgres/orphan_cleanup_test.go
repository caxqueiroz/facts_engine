package postgres_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func testOrphanCleanup(t *testing.T, ctx context.Context, normal, engine *facts.Engine, store *evidence.Postgres, c model.Caller) {
	t.Helper()
	refs := []model.ObjectRef{}
	for _, id := range []string{"cleanup-a", "cleanup-b", "cleanup-c"} {
		ref, err := store.PutPending(ctx, c.Tenant, model.PendingEvidenceUpload{ID: id, Scope: "case", PolicyID: "orphan-immediate"}, []byte(id))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	q := model.OrphanEvidenceQuery{Scope: "case", PolicyID: "orphan-immediate", Jurisdiction: "XX", Purpose: "cleanup", Limit: 2}
	page, err := engine.ScanOrphanEvidence(ctx, c, q)
	if err != nil || len(page) != 2 || page[0] != refs[0] || page[1] != refs[1] {
		t.Fatal("cleanup first page", page, err)
	}
	q.AfterLocator = page[1].Locator
	q.AfterVersion = page[1].Version
	page, err = engine.ScanOrphanEvidence(ctx, c, q)
	if err != nil || len(page) != 1 || page[0] != refs[2] {
		t.Fatal("cleanup next page", page, err)
	}
	q.AfterLocator = ""
	q.AfterVersion = ""
	q.PolicyID = "orphan-policy"
	page, err = engine.ScanOrphanEvidence(ctx, c, q)
	if err != nil || len(page) != 0 {
		t.Fatal("immature objects included", page, err)
	}
	q.PolicyID = "orphan-immediate"
	hold := model.LegalHoldRequest{Command: model.Command{Key: "cleanup-hold", Purpose: "cleanup"}, ID: "cleanup-hold", Scope: "case", Reason: "review"}
	if _, err := engine.PlaceLegalHold(ctx, c, hold); err != nil {
		t.Fatal(err)
	}
	page, err = engine.ScanOrphanEvidence(ctx, c, q)
	if err != nil || len(page) != 0 {
		t.Fatal("held objects included", page, err)
	}
	if _, err := engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: model.Command{Key: "cleanup-release", Purpose: "cleanup"}, ID: hold.ID, Scope: hold.Scope, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	// Remote disposal must share receipts across gRPC and Connect and bind tenant.
	grpc := v1.NewRetentionServiceClient(governanceGRPC(t, engine, c))
	grpcNormal := v1.NewRetentionServiceClient(governanceGRPC(t, normal, c))
	handler, err := connectapi.NewHandler(engine, func(context.Context) (model.Caller, error) { return c, nil }, connectapi.Limits{Timeout: 10 * time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := memoryv1connect.NewRetentionServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	ref := refs[0]
	request := &v1.DisposeOrphanEvidenceRequest{Tenant: c.Tenant, Command: &v1.Command{Key: "cleanup-remote", Purpose: "cleanup"}, Scope: "case", PolicyId: "orphan-immediate", Jurisdiction: "XX", Reason: "expired", Object: &v1.ObjectRef{Locator: ref.Locator, Version: ref.Version, Digest: ref.Digest, Size: ref.Size}}
	if result, err := grpcNormal.DisposeOrphanEvidence(ctx, request); result != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("runtime remote cleanup", result, err)
	}
	bad := proto.Clone(request).(*v1.DisposeOrphanEvidenceRequest)
	bad.Tenant = "other-tenant"
	if result, err := grpc.DisposeOrphanEvidence(ctx, bad); result != nil || grpcstatus.Code(err) != codes.PermissionDenied {
		t.Fatal("forged cleanup tenant", result, err)
	}
	bad = proto.Clone(request).(*v1.DisposeOrphanEvidenceRequest)
	bad.Object = nil
	if result, err := grpc.DisposeOrphanEvidence(ctx, bad); result != nil || grpcstatus.Code(err) != codes.InvalidArgument {
		t.Fatal("missing cleanup object", result, err)
	}
	first, err := grpc.DisposeOrphanEvidence(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := client.DisposeOrphanEvidence(ctx, request)
	if err != nil || !proto.Equal(first, replay) {
		t.Fatal("cleanup remote replay", replay, err)
	}
	bad = proto.Clone(request).(*v1.DisposeOrphanEvidenceRequest)
	bad.Reason = "changed"
	if _, err := client.DisposeOrphanEvidence(ctx, bad); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatal("cleanup changed replay", err)
	}
	caller := c
	caller.Service = true
	caller.Principal = "cleanup-worker"
	config := workers.OrphanCleanupConfig{Tenant: c.Tenant, Stream: "cleanup", Purpose: "cleanup", Reason: "expired", Bindings: []workers.OrphanCleanupBinding{{Scope: "case", PolicyID: "orphan-immediate", Jurisdiction: "XX"}}, PageSize: 1, MaxPages: 1, IntervalMinutes: 1}
	worker, err := workers.NewOrphanCleanup(engine, caller, config)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		result, err := worker.Tick(ctx)
		if err != nil || result.Scanned != 1 || result.Disposed != 1 || result.Skipped != 0 {
			t.Fatal("bounded cleanup pass", result, err)
		}
	}
	result, err := worker.Tick(ctx)
	if err != nil || result.Scanned != 0 {
		t.Fatal("cleanup replay scan", result, err)
	}
	for _, ref := range refs {
		if _, err := store.Read(ctx, c.Tenant, ref); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("cleanup retained bytes", err)
		}
	}
}
