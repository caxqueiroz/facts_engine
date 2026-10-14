package connectapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"strings"
	"testing"
	"time"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type retentionTestEngine struct {
	Engine
	calls int
}

func (e *retentionTestEngine) GetRetentionStatus(_ context.Context, caller model.Caller, q model.RetentionStatusQuery) (model.RetentionStatus, error) {
	e.calls++
	if caller.Tenant != "tenant" || q.Scope != "scope" || q.EvidenceID != "evidence" || q.Purpose != "review" {
		return model.RetentionStatus{}, model.ErrDenied
	}
	return model.RetentionStatus{EvidenceID: q.EvidenceID, PolicyID: "policy", RegisteredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), ActiveHolds: []string{"hold"}, Blockers: []string{"legal_hold"}}, nil
}
func TestRetentionGRPCValidationAndStatus(t *testing.T) {
	engine := &retentionTestEngine{}
	server, _ := startServer(t, engine, fixedIdentity, testLimits())
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	conn, err := grpc.NewClient(strings.TrimPrefix(server.URL, "https://"), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := v1.NewRetentionServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	query := &v1.GetRetentionStatusRequest{Tenant: "tenant", Scope: "scope", EvidenceId: "evidence", Purpose: "review"}
	result, err := client.GetRetentionStatus(ctx, query)
	if err != nil || result.Status.EvidenceId != "evidence" || len(result.Status.ActiveHolds) != 1 || result.Status.ActiveHolds[0] != "hold" || len(result.Status.Blockers) != 1 || result.Status.RegisteredAt.CheckValid() != nil {
		t.Fatal(result, err)
	}
	query.Tenant = "other"
	if _, err := client.GetRetentionStatus(ctx, query); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	query.Tenant = "tenant"
	query.Purpose = ""
	if _, err := client.GetRetentionStatus(ctx, query); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := client.RegisterRetentionPolicy(ctx, &v1.RegisterRetentionPolicyRequest{Tenant: "tenant"}); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if engine.calls != 1 {
		t.Fatal("rejected requests reached engine", engine.calls)
	}
}
