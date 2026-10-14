package mtlsidentity

import (
	"context"
	"crypto/tls"
	"github.com/caxqueiroz/facts_engine/internal/testtls"
	"github.com/caxqueiroz/facts_engine/model"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestVerifiedWorkloadIdentity(t *testing.T) {
	authority := testtls.New(t)
	serverCert := authority.Issue(t, true, nil, time.Now().Add(time.Hour))
	const uri = "spiffe://test/workload"
	bindings := []Binding{{URI: uri, Tenant: "tenant", Principal: "worker"}}
	auth, err := New(bindings)
	if err != nil {
		t.Fatal(err)
	}
	bindings[0].Principal = "changed"
	if _, err := auth.Verify(context.Background()); err == nil {
		t.Fatal("accepted missing middleware identity")
	}
	other, _ := New([]Binding{{URI: uri, Tenant: "other", Principal: "other"}})
	server := httptest.NewUnstartedServer(auth.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, err := auth.Verify(r.Context())
		if err != nil || caller != (model.Caller{Tenant: "tenant", Principal: "worker", Service: true}) {
			t.Errorf("identity: %+v, %v", caller, err)
		}
		if _, err := other.Verify(r.Context()); err == nil {
			t.Error("identity crossed authenticator instances")
		}
		if r.ProtoMajor != 2 {
			t.Error("expected HTTP/2")
		}
		w.WriteHeader(http.StatusNoContent)
	})))
	server.EnableHTTP2 = true
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCert.TLS}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: authority.Roots}
	server.StartTLS()
	defer server.Close()
	tests := []struct {
		name string
		want int
	}{
		{"valid", http.StatusNoContent}, {"unmapped", http.StatusUnauthorized}, {"ambiguous", http.StatusUnauthorized}, {"missing URI", http.StatusUnauthorized}, {"expired", 0}, {"untrusted", 0}, {"no certificate", 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: authority.Roots}
			uris := []string{uri}
			issuer := authority
			expires := time.Now().Add(time.Hour)
			switch test.name {
			case "unmapped":
				uris = []string{"spiffe://test/other"}
			case "ambiguous":
				uris = append(uris, "spiffe://test/other")
			case "missing URI":
				uris = nil
			case "expired":
				expires = time.Now().Add(-time.Minute)
			case "untrusted":
				issuer = testtls.New(t)
			}
			if test.name != "no certificate" {
				cert := issuer.Issue(t, false, uris, expires)
				tlsConfig.Certificates = []tls.Certificate{cert.TLS}
			}
			transport := &http.Transport{TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
			req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
			req.Header.Set("X-Tenant-ID", "forged")
			req.Header.Set("X-Principal-ID", "administrator")
			req.Header.Set("X-Initiator-ID", "human")
			req.Header.Set("Authorization", "Bearer forged")
			response, err := client.Do(req)
			if test.want == 0 {
				if err == nil {
					response.Body.Close()
					t.Fatal("invalid client certificate accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.want {
				t.Fatalf("status %d", response.StatusCode)
			}
		})
	}
}
func TestPlaintextAndInvalidBindings(t *testing.T) {
	auth, _ := New([]Binding{{URI: "spiffe://test/workload", Tenant: "tenant", Principal: "worker"}})
	response := httptest.NewRecorder()
	auth.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("plaintext reached handler") })).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://localhost/", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatal(response.Code)
	}
	for _, bindings := range [][]Binding{nil, {{URI: "spiffe://test/workload", Tenant: "invalid tenant", Principal: "worker"}}, {{URI: "https://user:secret@test/workload", Tenant: "tenant", Principal: "worker"}}, {{URI: "spiffe://test/workload", Tenant: "tenant", Principal: "worker"}, {URI: "spiffe://test/workload", Tenant: "tenant", Principal: "other"}}} {
		if _, err := New(bindings); err == nil {
			t.Fatal("invalid bindings accepted")
		}
	}
}
