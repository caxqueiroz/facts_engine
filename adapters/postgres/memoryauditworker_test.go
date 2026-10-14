package postgres_test

import (
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/internal/workerdaemon"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/protobuf/proto"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

func testStandaloneAudit(t *testing.T, ctx context.Context, client *http.Client, base, dsn, endpoint, storeID, modelID, tenant, principal string, public ed25519.PublicKey) {
	t.Helper()
	rpc := memoryv1connect.NewAuditServiceClient(connect.NewClient(connecthttp.NewTransport(client, base)))
	request := &v1.StartAuditExportRequest{Tenant: tenant, Scope: "case1", Command: &v1.Command{Key: "standalone-audit", Purpose: "integration-audit"}, IncludeEvidence: true}
	pending, err := rpc.StartAuditExport(ctx, request)
	if err != nil || pending.Result.State != "pending" {
		t.Fatalf("queued audit: %v, %v", pending, err)
	}
	t.Setenv("MEMORY_WORKER_DATABASE_URL", databaseRole(t, dsn, "facts_worker"))
	config := workerdaemon.Config{Mode: "audit", Principal: "audit-worker", Audit: workerdaemon.AuditConfig{Tenant: tenant, Stream: "exports", KeyID: "standalone-key"}, OpenFGAURL: endpoint, OpenFGAStore: storeID, OpenFGAModel: modelID, MaxConnections: 4, MaxAttempts: 3, LeaseSeconds: 3, PollMilliseconds: 10, MinBackoffMilliseconds: 10, MaxBackoffSeconds: 1, DeliveryTimeoutSeconds: 10}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- workerdaemon.Run(runCtx, config, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(6 * time.Second):
			t.Error("audit worker did not stop")
		}
	}()
	var completed *v1.AuditExportResult
	deadline := time.Now().Add(10 * time.Second)
	for {
		read, err := rpc.GetAuditExport(ctx, &v1.GetAuditExportRequest{Tenant: tenant, Scope: "case1", Id: pending.Result.Id, Purpose: "integration-audit"})
		if err != nil {
			t.Fatal(err)
		}
		if read.Result.State == "completed" {
			completed = read.Result
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("audit worker did not complete", read.Result.State)
		}
		time.Sleep(10 * time.Millisecond)
	}
	remote := completed.Bundle
	var manifest model.AuditManifest
	if err := json.Unmarshal(remote.ManifestJson, &manifest); err != nil {
		t.Fatal(err)
	}
	bundle := model.AuditBundle{Manifest: manifest, Signature: model.AuditSignature{Algorithm: remote.Signature.Algorithm, KeyID: remote.Signature.KeyId, Bytes: remote.Signature.Signature}}
	for _, entry := range remote.Entries {
		bundle.Entries = append(bundle.Entries, model.AuditEntry{Kind: entry.Kind, ID: entry.Id, Payload: entry.PayloadJson})
	}
	if err := audit.Verify(bundle, map[string]ed25519.PublicKey{"standalone-key": public}); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Authorization) == 0 {
		t.Fatal("missing requester authorization")
	}
	for _, decision := range manifest.Authorization {
		if decision.Request.Caller.Principal != principal || decision.Request.Caller.Tenant != tenant {
			t.Fatal("export ran as worker instead of original requester")
		}
	}
	again, err := rpc.StartAuditExport(ctx, request)
	if err != nil || !proto.Equal(again.Result, completed) {
		t.Fatalf("completed export replay: %v", err)
	}
}
