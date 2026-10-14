package postgres_test

import (
	"bytes"
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/adapters/mtlsidentity"
	"github.com/caxqueiroz/facts_engine/adapters/openfga"
	"github.com/caxqueiroz/facts_engine/authz"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/internal/daemon"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/internal/testtls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testMemoryd(t *testing.T, ctx context.Context, dsn string) {
	endpoint := os.Getenv("TEST_OPENFGA_URL")
	if endpoint == "" {
		t.Skip("set TEST_OPENFGA_URL for standalone service integration")
	}
	httpClient := &http.Client{Timeout: 5 * time.Second}
	post := func(path string, body any) map[string]json.RawMessage {
		t.Helper()
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+path, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := httpClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			t.Fatalf("OpenFGA provisioning status %d", response.StatusCode)
		}
		out := map[string]json.RawMessage{}
		b, err = io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > 0 {
			if err := json.Unmarshal(b, &out); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	str := func(value json.RawMessage) string {
		var out string
		if err := json.Unmarshal(value, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	store := str(post("/stores", map[string]string{"name": "memoryd-integration"})["id"])
	modelID := str(post("/stores/"+store+"/authorization-models", json.RawMessage(authz.ModelJSON))["authorization_model_id"])
	tenant := "memoryd-test"
	principal := "workload"
	scope := "case1"
	service := "service:" + openfga.ObjectID(tenant, principal)
	scopeObject := "scope:" + openfga.ObjectID(tenant, scope)
	tenantObject := "tenant:" + tenant
	tuple := func(user, relation, object string) map[string]string {
		return map[string]string{"user": user, "relation": relation, "object": object}
	}
	adminTuple := tuple(service, "administrator", scopeObject)
	post("/stores/"+store+"/write", map[string]any{"authorization_model_id": modelID, "writes": map[string]any{"tuple_keys": []map[string]string{tuple(tenantObject, "tenant", scopeObject), tuple(service, "member", tenantObject), tuple(service, "reader", scopeObject), tuple(service, "auditor", scopeObject), tuple(service, "operator", scopeObject), adminTuple}}})
	auditWorker := "service:" + openfga.ObjectID(tenant, "audit-worker")
	post("/stores/"+store+"/write", map[string]any{"authorization_model_id": modelID, "writes": map[string]any{"tuple_keys": []map[string]string{tuple(auditWorker, "member", tenantObject), tuple(auditWorker, "operator", scopeObject), tuple(auditWorker, "auditor", scopeObject)}}})
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("MEMORY_AUDIT_PRIVATE_KEY_HEX", hex.EncodeToString(private))
	route := standaloneRoute(t, ctx, tenant)
	t.Setenv("MEMORY_OPERATIONS_DATABASE_URL", databaseRole(t, dsn, "facts_rebuild"))
	authority := testtls.New(t)
	serverCert := authority.Issue(t, true, nil, time.Now().Add(time.Hour))
	clientCert := authority.Issue(t, false, []string{"spiffe://test/memoryd"}, time.Now().Add(time.Hour))
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	config := daemon.Config{AuditKeyID: "standalone-key", Retrieval: []runtimeconfig.LexicalRoute{route}, OperationsScope: scope, Listen: listener.Addr().String(), TLSCertificate: write("server.pem", serverCert.PEM), TLSKey: write("server.key", serverCert.KeyPEM), ClientCA: write("ca.pem", authority.PEM), Identities: []mtlsidentity.Binding{{URI: "spiffe://test/memoryd", Tenant: tenant, Principal: principal}}, OpenFGAURL: endpoint, OpenFGAStore: store, OpenFGAModel: modelID, ReadinessScope: scope, MaxConnections: 4, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20, RPCTimeoutSeconds: 5, ShutdownSeconds: 3}
	runtimeURL, err := url.Parse(dsn)
	if err != nil || runtimeURL.Scheme == "" {
		t.Fatal("integration requires a PostgreSQL URL")
	}
	parameters := runtimeURL.Query()
	parameters.Set("role", "facts_runtime")
	runtimeURL.RawQuery = parameters.Encode()
	t.Setenv("MEMORY_DATABASE_URL", runtimeURL.String())
	t.Setenv("MEMORY_ENCRYPTION_KEY_HEX", strings.Repeat("04", 32))
	t.Setenv("MEMORY_PAGINATION_KEY_HEX", strings.Repeat("ab", 32))
	t.Setenv("MEMORY_OPENFGA_TOKEN", "")
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- daemon.RunOnListener(runCtx, config, slog.New(slog.NewTextHandler(io.Discard, nil)), listener)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("memoryd did not stop")
		}
	})
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: authority.Roots, Certificates: []tls.Certificate{clientCert.TLS}}, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	base := "https://" + config.Listen
	// The pre-bound listener queues this connection while startup validates schema.
	ready, err := client.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(ready.Body)
	ready.Body.Close()
	if err != nil || ready.StatusCode != http.StatusOK || !bytes.Contains(data, []byte(`"authorization":true`)) || ready.ProtoMajor != 2 {
		t.Fatalf("readiness: %d %s %v", ready.StatusCode, data, err)
	}
	identity := memoryv1connect.NewIdentityServiceClient(connect.NewClient(connecthttp.NewTransport(client, base)))
	request := &v1.PutScopeRequest{Tenant: tenant, Command: &v1.Command{Key: "create-scope", Purpose: "integration"}, Definition: &v1.ScopeDefinition{Id: scope, Label: "Service scope"}, Reason: "Integration fixture"}
	written, err := identity.PutScope(ctx, request)
	if err != nil || written.Result.StreamRevision != 1 {
		t.Fatalf("service write: %v, %v", written, err)
	}
	read, err := identity.GetScope(ctx, &v1.GetScopeRequest{Tenant: tenant, Scope: scope, Purpose: "integration"})
	if err != nil || read.Version.Caller.Principal != principal || !read.Version.Caller.Service || read.Version.Caller.Initiator != "" {
		t.Fatalf("verified service identity: %v, %v", read, err)
	}
	testStandaloneProjection(t, ctx, client, base, dsn, endpoint, store, modelID, principal, route)
	testStandaloneAudit(t, ctx, client, base, dsn, endpoint, store, modelID, tenant, principal, public)
	request.Tenant = "bank1"
	if _, err := identity.PutScope(ctx, request); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("tenant mismatch: %v", err)
	}
	request.Tenant = tenant
	request.Command.Key = "after-revocation"
	request.ExpectedRevision = 1
	post("/stores/"+store+"/write", map[string]any{"authorization_model_id": modelID, "deletes": map[string]any{"tuple_keys": []map[string]string{adminTuple}}})
	if _, err := identity.PutScope(ctx, request); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("revoked administrative access: %v", err)
	}
}
