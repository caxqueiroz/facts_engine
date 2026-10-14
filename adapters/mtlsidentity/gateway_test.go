package mtlsidentity

import (
	"crypto/tls"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/internal/testtls"
	"github.com/caxqueiroz/facts_engine/model"
)

func TestGatewayIdentityOverMutualTLS(t *testing.T) {
	authority := testtls.New(t)
	serverCert := authority.Issue(t, true, nil, time.Now().Add(time.Hour))
	gateways := []GatewayBinding{{URI: "spiffe://test/gateway", Tenant: "tenant"}}
	auth, err := NewWithGateways([]Binding{{URI: "spiffe://test/worker", Tenant: "tenant", Principal: "worker"}}, gateways)
	if err != nil {
		t.Fatal(err)
	}
	gateways[0].Tenant = "changed"
	server := httptest.NewUnstartedServer(auth.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, err := auth.Verify(r.Context())
		if err != nil {
			t.Error(err)
			return
		}
		if r.ProtoMajor != 2 {
			t.Error("expected HTTP/2")
		}
		_ = json.NewEncoder(w).Encode(caller)
	})))
	server.EnableHTTP2 = true
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCert.TLS}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: authority.Roots}
	server.StartTLS()
	defer server.Close()
	for _, test := range []struct {
		name, uri           string
		tenants, principals []string
		want                model.Caller
	}{
		{"user", "gateway", []string{"tenant"}, []string{"user"}, model.Caller{Tenant: "tenant", Principal: "user"}},
		{"workload ignores headers", "worker", []string{"other"}, []string{"admin"}, model.Caller{Tenant: "tenant", Principal: "worker", Service: true}},
		{"missing tenant", "gateway", nil, []string{"user"}, model.Caller{}},
		{"wrong tenant", "gateway", []string{"other"}, []string{"user"}, model.Caller{}},
		{"duplicate tenant", "gateway", []string{"tenant", "tenant"}, []string{"user"}, model.Caller{}},
		{"missing principal", "gateway", []string{"tenant"}, nil, model.Caller{}},
		{"duplicate principal", "gateway", []string{"tenant"}, []string{"user", "admin"}, model.Caller{}},
		{"combined principal", "gateway", []string{"tenant"}, []string{"user,admin"}, model.Caller{}},
		{"space principal", "gateway", []string{"tenant"}, []string{"invalid user"}, model.Caller{}},
		{"oversized principal", "gateway", []string{"tenant"}, []string{strings.Repeat("a", 129)}, model.Caller{}},
		{"unmapped certificate", "unknown", []string{"tenant"}, []string{"user"}, model.Caller{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cert := authority.Issue(t, false, []string{"spiffe://test/" + test.uri}, time.Now().Add(time.Hour))
			transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: authority.Roots, Certificates: []tls.Certificate{cert.TLS}}, ForceAttemptHTTP2: true}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: time.Second}
			request, err := http.NewRequest(http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range test.tenants {
				request.Header.Add(VerifiedTenantHeader, value)
			}
			for _, value := range test.principals {
				request.Header.Add(VerifiedPrincipalHeader, value)
			}
			request.Header.Set("X-Facts-Service", "true")
			request.Header.Set("X-Facts-Initiator", "forged")
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if test.want == (model.Caller{}) {
				if response.StatusCode != http.StatusUnauthorized {
					t.Fatal("untrusted assertion accepted", response.StatusCode)
				}
				return
			}
			var caller model.Caller
			if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&caller) != nil || caller != test.want {
				t.Fatal("wrong authenticated caller", response.StatusCode, caller)
			}
		})
	}
}

func TestGatewayConfigurationAndAmbiguousHeaders(t *testing.T) {
	binding := []Binding{{URI: "spiffe://test/worker", Tenant: "tenant", Principal: "worker"}}
	for _, gateways := range [][]GatewayBinding{
		{{URI: "spiffe://test/worker", Tenant: "tenant"}},
		{{URI: "spiffe://test/gateway", Tenant: "tenant"}, {URI: "spiffe://test/gateway", Tenant: "other"}},
		{{URI: "https://user:password@example.test", Tenant: "tenant"}},
		{{URI: "spiffe://test/gateway?", Tenant: "tenant"}},
		{{URI: "spiffe://test/gateway", Tenant: "bad tenant"}},
	} {
		if _, err := NewWithGateways(binding, gateways); err == nil {
			t.Fatal("invalid gateway configuration accepted")
		}
	}
	if _, err := NewWithGateways(nil, []GatewayBinding{{URI: "spiffe://test/gateway", Tenant: "tenant"}}); err == nil {
		t.Fatal("missing workload binding accepted")
	}
	header := http.Header{VerifiedPrincipalHeader: []string{"user"}, strings.ToLower(VerifiedPrincipalHeader): []string{"admin"}}
	if singleHeader(header, VerifiedPrincipalHeader) != "" {
		t.Fatal("case-variant duplicate accepted")
	}
}
