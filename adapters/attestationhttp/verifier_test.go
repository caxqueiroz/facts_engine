package attestationhttp

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func identityRequest() model.AttestationIdentityRequest {
	return model.AttestationIdentityRequest{Recorder: model.Caller{Tenant: "tenant", Principal: "recorder", Service: true}, Request: model.DelegatedAttestationRequest{GrantID: "grant", AttestationRequest: model.AttestationRequest{Command: model.Command{Key: "command", Purpose: "confirmation"}, Attestation: model.Attestation{ID: "attestation", Scope: "scope", RecordID: "record", VersionID: "version", ContentDigest: "content", Kind: "client_confirmation", Attestor: model.Caller{Tenant: "tenant", Principal: "client"}}}}}
}
func receiptFor(r model.AttestationIdentityRequest) model.AttestationIdentityReceipt {
	now := time.Now().UTC()
	return model.AttestationIdentityReceipt{Authority: "authority", BindingID: "binding", RequestDigest: model.AttestationIdentityDigest(r), Attestor: r.Request.Attestation.Attestor, AuthenticatedAt: now.Add(-time.Minute), VerifiedAt: now, ExpiresAt: now.Add(time.Minute)}
}
func verifierFor(t *testing.T, server *httptest.Server) *Verifier {
	t.Helper()
	verifier, err := New(Config{URL: server.URL, Authority: "authority", Token: "private-token", RootCAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(verifier.Close)
	return verifier
}
func TestBoundAuthorityRequestAndReceipt(t *testing.T) {
	request := identityRequest()
	want := receiptFor(request)
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer private-token" || r.Header.Get("X-Facts-Attestation-Identity-Protocol") != "v1" || r.Header.Get("Cache-Control") != "no-store" {
			t.Error("authority protocol or credentials missing")
		}
		var envelope Request
		if json.NewDecoder(r.Body).Decode(&envelope) != nil || envelope.RequestDigest != model.AttestationIdentityDigest(envelope.Request) || (calls.Add(1) == 1 && !reflect.DeepEqual(envelope.Request, request)) {
			t.Error("complete identity request not bound")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer server.Close()
	verifier := verifierFor(t, server)
	got, err := verifier.VerifyAttestationIdentity(context.Background(), request)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("receipt changed", err)
	}
	// A malicious response replay cannot authorize changed command content.
	changed := request
	changed.Request.Purpose = "different"
	if _, err := verifier.VerifyAttestationIdentity(context.Background(), changed); !errors.Is(err, model.ErrDenied) {
		t.Fatal("unbound receipt accepted", err)
	}
}
func TestAuthorityResponseRejection(t *testing.T) {
	request := identityRequest()
	receipt, _ := json.Marshal(receiptFor(request))
	for _, test := range []struct {
		name, body, contentType string
		status                  int
		want                    error
	}{
		{"denial", "private authority error", "text/plain", 403, model.ErrDenied},
		{"credentials", "private authority error", "text/plain", 401, model.ErrUnavailable},
		{"outage", "private authority error", "text/plain", 503, model.ErrUnavailable},
		{"malformed", "{", "application/json", 200, model.ErrUnavailable},
		{"trailing", string(receipt) + " {}", "application/json", 200, model.ErrUnavailable},
		{"unknown", strings.TrimSuffix(string(receipt), "}") + ",\"unknown\":1}", "application/json", 200, model.ErrUnavailable},
		{"duplicate", strings.Replace(string(receipt), "\"Authority\":", "\"authority\":\"other\",\"Authority\":", 1), "application/json", 200, model.ErrUnavailable},
		{"oversized", strings.Repeat(" ", (64<<10)+1), "application/json", 200, model.ErrUnavailable},
		{"wrong type", string(receipt), "text/plain", 200, model.ErrUnavailable},
		{"wrong authority", strings.Replace(string(receipt), "\"authority\"", "\"other\"", 1), "application/json", 200, model.ErrDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", test.contentType)
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			_, err := verifierFor(t, server).VerifyAttestationIdentity(context.Background(), request)
			if !errors.Is(err, test.want) || calls.Load() != 1 || strings.Contains(err.Error(), "private") {
				t.Fatal("incorrect or unsafe authority error", err)
			}
		})
	}
}
func TestAuthorityRedirectCancellationAndTrust(t *testing.T) {
	var redirected atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Store(true) }))
	defer destination.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	verifier := verifierFor(t, server)
	if _, err := verifier.VerifyAttestationIdentity(context.Background(), identityRequest()); !errors.Is(err, model.ErrUnavailable) || redirected.Load() {
		t.Fatal("redirect accepted", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := verifier.VerifyAttestationIdentity(ctx, identityRequest()); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	untrusted, err := New(Config{URL: server.URL, Authority: "authority", Token: "private-token", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.Close()
	if _, err := untrusted.VerifyAttestationIdentity(context.Background(), identityRequest()); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("untrusted TLS accepted", err)
	}
}
func TestAuthorityConfigurationAndJSONAmbiguity(t *testing.T) {
	for _, endpoint := range []string{"http://127.0.0.1", "https://user:password@example.test", "https://example.test?secret=value", "https://example.test#fragment", "https://example.test?"} {
		if _, err := New(Config{URL: endpoint, Authority: "authority", Token: "token", Timeout: time.Second}); err == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
	for _, raw := range []string{`{"Attestor":{"Principal":"one","principal":"two"}}`, `{"ExpiresAt":"one","ExpireſAt":"two"}`, `{} {}`, strings.Repeat("[", 10) + "0" + strings.Repeat("]", 10)} {
		if unambiguousJSON([]byte(raw)) {
			t.Fatal("ambiguous JSON accepted")
		}
	}
}

func TestAuthorityRequestLimitAndResponseDeadline(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var envelope Request
		if json.NewDecoder(r.Body).Decode(&envelope) != nil {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{"))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer server.Close()
	verifier := verifierFor(t, server)
	request := identityRequest()
	request.Request.Purpose = strings.Repeat("x", 1<<20)
	if _, err := verifier.VerifyAttestationIdentity(context.Background(), request); !errors.Is(err, model.ErrInvalid) || calls.Load() != 0 {
		t.Fatal("oversized input reached authority", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := verifier.VerifyAttestationIdentity(ctx, identityRequest()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("response read ignored deadline", err)
	}
}
