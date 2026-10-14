package extractionhttp

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func metadata(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Facts-Model-Identity", "fixture-v1")
	w.Header().Set("X-Facts-Input-Tokens", "7")
	w.Header().Set("X-Facts-Output-Tokens", "3")
}
func TestExactGatewayBytesAndTLS(t *testing.T) {
	input := []byte("{\n  \"prompt\": \"opaque\"\n}")
	output := []byte("{ \"candidates\": [] }\n")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, input) || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer test-secret" || r.Header.Get("X-Facts-Extraction-Protocol") != "v1" {
			t.Error("gateway request changed")
		}
		metadata(w)
		_, _ = w.Write(output)
	}))
	defer server.Close()
	c := Config{URL: server.URL, Token: "test-secret", RootCAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), Timeout: time.Second, MaxInputBytes: 1024, MaxOutputBytes: 1024}
	p, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	result, err := p.Extract(context.Background(), input)
	if err != nil || !bytes.Equal(result.Body, output) || result.ModelIdentity != "fixture-v1" || result.InputTokens != 7 || result.OutputTokens != 3 {
		t.Fatal(result, err)
	}
	c.RootCAPEM = nil
	untrusted, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.Close()
	if _, err = untrusted.Extract(context.Background(), input); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("untrusted TLS accepted", err)
	}
}
func TestGatewayBoundsAndFailures(t *testing.T) {
	for _, scenario := range []string{"oversize", "redirect", "missing usage", "negative usage", "duplicate model", "compressed", "error"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				metadata(w)
				switch scenario {
				case "redirect":
					w.Header().Set("Location", "/again")
					w.WriteHeader(302)
					return
				case "missing usage":
					w.Header().Del("X-Facts-Input-Tokens")
				case "negative usage":
					w.Header().Set("X-Facts-Output-Tokens", "-1")
				case "duplicate model":
					w.Header().Add("X-Facts-Model-Identity", "other")
				case "compressed":
					w.Header().Set("Content-Encoding", "gzip")
				case "error":
					w.WriteHeader(500)
				}
				_, _ = w.Write(bytes.Repeat([]byte("x"), 100))
			}))
			defer server.Close()
			p, err := New(Config{URL: server.URL, AllowPlaintextLoopback: true, Timeout: time.Second, MaxInputBytes: 4, MaxOutputBytes: 8})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			result, err := p.Extract(context.Background(), []byte("{}"))
			if scenario == "oversize" {
				if err != nil || len(result.Body) != 9 {
					t.Fatal("unbounded response", len(result.Body), err)
				}
			} else if !errors.Is(err, model.ErrUnavailable) {
				t.Fatal("invalid gateway response accepted", scenario, err)
			}
			if _, err := p.Extract(context.Background(), []byte("oversize")); !errors.Is(err, model.ErrInvalid) {
				t.Fatal("input limit ignored", err)
			}
		})
	}
}
func TestGatewayCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metadata(w)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	p, err := New(Config{URL: server.URL, AllowPlaintextLoopback: true, Timeout: time.Second, MaxInputBytes: 8, MaxOutputBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := p.Extract(ctx, []byte("{}")); err == nil || time.Since(start) > time.Second {
		t.Fatal("cancellation ignored", err)
	}
}
func TestGatewayEndpointConfiguration(t *testing.T) {
	for _, endpoint := range []string{"http://example.com", "http://localhost", "https://user:secret@example.com", "https://example.com?secret=x", "https://example.com#fragment", "ftp://example.com"} {
		if _, err := New(Config{URL: endpoint, AllowPlaintextLoopback: true, Timeout: time.Second, MaxInputBytes: 8, MaxOutputBytes: 8}); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("invalid endpoint accepted", endpoint, err)
		}
	}
	if _, err := New(Config{URL: "http://127.0.0.1", Timeout: time.Second, MaxInputBytes: 8, MaxOutputBytes: 8}); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("implicit plaintext allowed")
	}
}
