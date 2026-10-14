// Package attestationhttp calls an explicitly trusted identity authority using
// the facts attestation identity v1 protocol. It does not issue identity grants.
package attestationhttp

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

type Config struct {
	URL, Authority, Token string
	RootCAPEM             []byte
	Timeout               time.Duration
}

func (c Config) Validate() error {
	u, err := url.Parse(c.URL)
	if err != nil || len(c.URL) > 2048 || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || !model.ValidID(c.Authority) || c.Timeout < time.Second || c.Timeout > 30*time.Second || len(c.Token) == 0 || len(c.Token) > 8192 || len(c.RootCAPEM) > 1<<20 {
		return model.ErrInvalid
	}
	for _, r := range c.Token {
		if r < 0x21 || r > 0x7e {
			return model.ErrInvalid
		}
	}
	return nil
}

// Request is the v1 authority request envelope. Digest covers every field in Request.
type Request struct {
	RequestDigest string                           `json:"request_digest"`
	Request       model.AttestationIdentityRequest `json:"request"`
}
type Verifier struct {
	endpoint, authority, token string
	client                     *http.Client
	transport                  *http.Transport
}

func New(c Config) (*Verifier, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	var roots *x509.CertPool
	if len(c.RootCAPEM) > 0 {
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(c.RootCAPEM) {
			return nil, model.ErrInvalid
		}
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}, DialContext: (&net.Dialer{Timeout: c.Timeout, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: c.Timeout, ResponseHeaderTimeout: c.Timeout, MaxResponseHeaderBytes: 16 << 10, MaxConnsPerHost: 4, MaxIdleConns: 4, MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second, DisableCompression: true}
	client := &http.Client{Transport: transport, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Verifier{endpoint: c.URL, authority: c.Authority, token: c.Token, client: client, transport: transport}, nil
}
func (v *Verifier) Close() { v.transport.CloseIdleConnections() }
func (v *Verifier) VerifyAttestationIdentity(ctx context.Context, r model.AttestationIdentityRequest) (model.AttestationIdentityReceipt, error) {
	var empty model.AttestationIdentityReceipt
	if !model.ValidID(r.Request.GrantID) || !model.ValidID(r.Recorder.Tenant) || !model.ValidID(r.Recorder.Principal) || !r.Recorder.Service || r.Request.Attestation.Attestor.Tenant != r.Recorder.Tenant {
		return empty, model.ErrInvalid
	}
	digest := model.AttestationIdentityDigest(r)
	body, err := json.Marshal(Request{RequestDigest: digest, Request: r})
	if err != nil || len(body) > 1<<20 {
		return empty, model.ErrInvalid
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, v.endpoint, bytes.NewReader(body))
	if err != nil {
		return empty, model.ErrUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+v.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("X-Facts-Attestation-Identity-Protocol", "v1")
	response, err := v.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return empty, ctx.Err()
		}
		return empty, model.ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusForbidden {
		return empty, model.ErrDenied
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK || len(response.Header.Values("Content-Type")) != 1 || err != nil || media != "application/json" || response.Header.Get("Content-Encoding") != "" {
		return empty, model.ErrUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if ctx.Err() != nil {
		return empty, ctx.Err()
	}
	if err != nil || len(raw) > 64<<10 {
		return empty, model.ErrUnavailable
	}
	if !unambiguousJSON(raw) {
		return empty, model.ErrUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var receipt model.AttestationIdentityReceipt
	if decoder.Decode(&receipt) != nil {
		return empty, model.ErrUnavailable
	}
	if receipt.Authority != v.authority || !model.ValidID(receipt.BindingID) || receipt.RequestDigest != digest || receipt.Attestor != r.Request.Attestation.Attestor {
		return empty, model.ErrDenied
	}
	// The engine independently validates authentication age, receipt expiry and
	// confirmation timestamps, and rechecks freshness inside the committing transaction.
	return receipt, nil
}

// Reject duplicate object fields (including case aliases), trailing values and
// excessive nesting before decoding the typed receipt. Never include raw JSON in errors.
func unambiguousJSON(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 8 {
			return errors.New("nesting")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		if delimiter != '{' && delimiter != '[' {
			return errors.New("delimiter")
		}
		seen := map[string]bool{}
		for decoder.More() {
			if delimiter == '{' {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok {
					return errors.New("key")
				}
				for _, character := range name {
					if character > 127 {
						return errors.New("non-ASCII field")
					}
				}
				name = strings.ToLower(name)
				if seen[name] {
					return errors.New("duplicate")
				}
				seen[name] = true
			}
			if err := walk(depth + 1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	}
	if walk(0) != nil {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}

var _ model.AttestationIdentityVerifier = (*Verifier)(nil)
