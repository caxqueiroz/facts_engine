package connectapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testEngine struct {
	FactEngine
	calls atomic.Int64
	get   func(context.Context, model.Caller, model.FactQuery) (model.FactResult, error)
}

func (e *testEngine) GetFact(ctx context.Context, c model.Caller, q model.FactQuery) (model.FactResult, error) {
	e.calls.Add(1)
	return e.get(ctx, c, q)
}
func testLimits() Limits {
	return Limits{Timeout: time.Second, MaxConcurrent: 4, MaxRequestBytes: 1 << 20, MaxResponseBytes: 1 << 20}
}
func validQuery() *v1.GetFactRequest {
	return &v1.GetFactRequest{Tenant: "tenant", Purpose: "review", Key: &v1.FactKey{Scope: "scope", Subject: "subject", Predicate: "predicate"}, ValidAt: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))}
}
func startServer(t *testing.T, e FactEngine, verify IdentityVerifier, limits Limits) (*httptest.Server, memoryv1connect.FactServiceClient) {
	t.Helper()
	handler, err := NewHandler(e, verify, limits)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	client := memoryv1connect.NewFactServiceClient(connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL)))
	return server, client
}
func fixedIdentity(context.Context) (model.Caller, error) {
	return model.Caller{Tenant: "tenant", Principal: "human"}, nil
}

func TestConnectAndGRPC(t *testing.T) {
	engine := &testEngine{get: func(_ context.Context, c model.Caller, q model.FactQuery) (model.FactResult, error) {
		if c.Tenant != "tenant" || c.Principal != "human" || q.Key.Subject != "subject" {
			return model.FactResult{}, errors.New("incorrect mapping")
		}
		if q.Purpose == "conflict" {
			return model.FactResult{}, &model.RevisionConflict{RecordID: "record", ExpectedRevision: 1, CurrentRevision: 2}
		}
		if q.Purpose == "denied" {
			return model.FactResult{}, model.ErrDenied
		}
		b := false
		return model.FactResult{State: "accepted", Value: &model.Value{Kind: model.Boolean, Boolean: &b}, LedgerRevision: 42}, nil
	}}
	server, client := startServer(t, engine, fixedIdentity, testLimits())
	got, err := client.GetFact(context.Background(), validQuery())
	if err != nil || got.Result.LedgerRevision != 42 {
		t.Fatalf("Connect: %v, %v", got, err)
	}
	if _, ok := got.Result.Value.Payload.(*v1.Value_Boolean); !ok {
		t.Fatal("false lost presence")
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	conn, err := grpc.NewClient(strings.TrimPrefix(server.URL, "https://"), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	grpcClient := v1.NewFactServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g, err := grpcClient.GetFact(ctx, validQuery())
	if err != nil || g.Result.LedgerRevision != 42 {
		t.Fatalf("gRPC: %v, %v", g, err)
	}
	denied := validQuery()
	denied.Purpose = "denied"
	if _, err := grpcClient.GetFact(ctx, denied); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("gRPC error: %v", err)
	}
	conflict := validQuery()
	conflict.Purpose = "conflict"
	_, err = grpcClient.GetFact(ctx, conflict)
	if status.Code(err) != codes.Aborted || len(status.Convert(err).Details()) != 1 {
		t.Fatalf("gRPC conflict: %v; details=%v", err, status.Convert(err).Details())
	}
	_, err = client.GetFact(ctx, conflict)
	var rpc *connect.Error
	if !errors.As(err, &rpc) || len(rpc.Details()) != 1 {
		t.Fatalf("Connect conflict: %v; details=%v", err, rpc.Details())
	}

}

func TestIdentityAndValidationBeforeEngine(t *testing.T) {
	engine := &testEngine{get: func(context.Context, model.Caller, model.FactQuery) (model.FactResult, error) {
		return model.FactResult{}, nil
	}}
	_, client := startServer(t, engine, fixedIdentity, testLimits())
	for _, tc := range []struct {
		name   string
		change func(*v1.GetFactRequest)
		code   connect.Code
	}{
		{"tenant", func(r *v1.GetFactRequest) { r.Tenant = "other" }, connect.CodePermissionDenied},
		{"key", func(r *v1.GetFactRequest) { r.Key = nil }, connect.CodeInvalidArgument},
		{"purpose", func(r *v1.GetFactRequest) { r.Purpose = "" }, connect.CodeInvalidArgument},
		{"timestamp", func(r *v1.GetFactRequest) { r.ValidAt = &timestamppb.Timestamp{Nanos: 1000000000} }, connect.CodeInvalidArgument},
		{"boundary", func(r *v1.GetFactRequest) { r.KnownAt = r.ValidAt; r.KnowledgeRevision = 1 }, connect.CodeInvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := validQuery()
			tc.change(r)
			_, err := client.GetFact(context.Background(), r)
			if connect.CodeOf(err) != tc.code {
				t.Fatalf("got %v", err)
			}
		})
	}
	if engine.calls.Load() != 0 {
		t.Fatal("invalid requests reached engine")
	}
	_, unauth := startServer(t, engine, func(context.Context) (model.Caller, error) { return model.Caller{}, errors.New("private credential") }, testLimits())
	_, err := unauth.GetFact(context.Background(), &v1.GetFactRequest{})
	if connect.CodeOf(err) != connect.CodeUnauthenticated || strings.Contains(err.Error(), "private") {
		t.Fatalf("authentication: %v", err)
	}
}

func TestDeadlineAndErrorSanitization(t *testing.T) {
	engine := &testEngine{get: func(ctx context.Context, _ model.Caller, _ model.FactQuery) (model.FactResult, error) {
		<-ctx.Done()
		return model.FactResult{}, ctx.Err()
	}}
	limits := testLimits()
	limits.Timeout = 30 * time.Millisecond
	_, client := startServer(t, engine, fixedIdentity, limits)
	_, err := client.GetFact(context.Background(), validQuery())
	if connect.CodeOf(err) != connect.CodeDeadlineExceeded {
		t.Fatalf("deadline: %v", err)
	}
	if err := rpcError(errors.New("secret sql password")); connect.CodeOf(err) != connect.CodeInternal || strings.Contains(err.Error(), "secret") {
		t.Fatalf("internal: %v", err)
	}
}

func TestAdmissionAndRequestSize(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	engine := &testEngine{get: func(ctx context.Context, _ model.Caller, _ model.FactQuery) (model.FactResult, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return model.FactResult{}, nil
		case <-ctx.Done():
			return model.FactResult{}, ctx.Err()
		}
	}}
	limits := testLimits()
	limits.MaxConcurrent = 1
	limits.Timeout = 5 * time.Second
	_, client := startServer(t, engine, fixedIdentity, limits)
	done := make(chan error, 1)
	go func() { _, err := client.GetFact(context.Background(), validQuery()); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not enter")
	}
	_, err := client.GetFact(context.Background(), validQuery())
	close(release)
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("admission: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	limits.MaxRequestBytes = 32
	_, small := startServer(t, engine, fixedIdentity, limits)
	_, err = small.GetFact(context.Background(), validQuery())
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("request bytes: %v", err)
	}
	if engine.calls.Load() != 1 {
		t.Fatal("rejected request entered engine")
	}
}
