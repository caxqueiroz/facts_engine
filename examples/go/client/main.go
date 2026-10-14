// This client uses a host-issued workload certificate. Tenant binding and
// OpenFGA permissions are checked by the service independently of this input.
package main

import (
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"net/http"
	"net/url"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "remote request failed; verify certificate, endpoint and permissions")
		os.Exit(1)
	}
}
func run() error {
	endpoint := os.Getenv("MEMORY_SERVICE_URL")
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("invalid HTTPS endpoint")
	}
	cert, err := tls.LoadX509KeyPair(os.Getenv("MEMORY_TLS_CERT"), os.Getenv("MEMORY_TLS_KEY"))
	if err != nil {
		return err
	}
	ca, err := os.ReadFile(os.Getenv("MEMORY_SERVER_CA"))
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid server CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{cert}}, ForceAttemptHTTP2: true, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second}
	defer transport.CloseIdleConnections()
	client := memoryv1connect.NewIdentityServiceClient(connect.NewClient(connecthttp.NewTransport(&http.Client{Transport: transport, Timeout: 10 * time.Second}, endpoint)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := client.GetScope(ctx, &v1.GetScopeRequest{Tenant: os.Getenv("TENANT_ID"), Scope: os.Getenv("SCOPE_ID"), Purpose: "application-read"})
	if err != nil {
		return err
	}
	fmt.Printf("metadata_revision=%d ledger_revision=%d\n", response.Version.Revision, response.Version.LedgerRevision)
	return nil
}
