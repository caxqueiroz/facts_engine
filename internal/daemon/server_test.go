package daemon

import (
	"context"
	"crypto/tls"
	"github.com/caxqueiroz/facts_engine/adapters/mtlsidentity"
	"github.com/caxqueiroz/facts_engine/internal/testtls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestServeReadinessAndGracefulShutdown(t *testing.T) {
	authority := testtls.New(t)
	serverCert := authority.Issue(t, true, nil, time.Now().Add(time.Hour))
	clientCert := authority.Issue(t, false, []string{"spiffe://test/worker"}, time.Now().Add(time.Hour))
	auth, _ := mtlsidentity.New([]mtlsidentity.Binding{{URI: "spiffe://test/worker", Tenant: "tenant", Principal: "worker"}})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: authority.Roots, Certificates: []tls.Certificate{clientCert.TLS}}, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	base := "https://" + listener.Addr().String()
	entered, release := make(chan struct{}), make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
			t.Error("graceful shutdown canceled active request")
		}
	})
	var healthy atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, listener, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCert.TLS}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: authority.Roots}, handler, auth.Wrap, func(context.Context) map[string]bool {
			return map[string]bool{"ledger": healthy.Load(), "authorization": true}
		}, time.Second, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	check := func(path string, want int, contains string) {
		t.Helper()
		response, err := client.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		b, _ := io.ReadAll(response.Body)
		if response.StatusCode != want || !strings.Contains(string(b), contains) {
			t.Fatalf("health: %d %s", response.StatusCode, b)
		}
	}
	check("/livez", http.StatusOK, "true")
	check("/readyz", http.StatusServiceUnavailable, "\"ledger\":false")
	healthy.Store(true)
	check("/readyz", http.StatusOK, "\"ready\":true")
	requestDone := make(chan error, 1)
	go func() {
		response, err := client.Get(base + "/work")
		if err == nil {
			response.Body.Close()
			if response.StatusCode != http.StatusNoContent {
				err = io.ErrUnexpectedEOF
			}
		}
		requestDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request never entered")
	}
	cancel()
	close(release)
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func TestForcedShutdownCancelsRequests(t *testing.T) {
	authority := testtls.New(t)
	serverCert := authority.Issue(t, true, nil, time.Now().Add(time.Hour))
	clientCert := authority.Issue(t, false, []string{"spiffe://test/worker"}, time.Now().Add(time.Hour))
	auth, _ := mtlsidentity.New([]mtlsidentity.Binding{{URI: "spiffe://test/worker", Tenant: "tenant", Principal: "worker"}})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: authority.Roots, Certificates: []tls.Certificate{clientCert.TLS}}, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	base := "https://" + listener.Addr().String()
	entered, canceled := make(chan struct{}), make(chan struct{})
	handler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(canceled) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, listener, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCert.TLS}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: authority.Roots}, handler, auth.Wrap, func(context.Context) map[string]bool { return map[string]bool{"ledger": true} }, time.Second, 20*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, err := client.Get(base + "/work")
		if err == nil {
			response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request never entered")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("forced shutdown reported graceful success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown unbounded")
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("request not canceled")
	}
	<-requestDone
}
