// Package extractionhttp implements the facts extraction gateway v1 contract.
// A host-approved gateway receives exact retained request bytes and returns raw
// candidate JSON with model identity and usage in response headers.
package extractionhttp

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

type Config struct {
	URL, Token                    string
	RootCAPEM                     []byte
	AllowPlaintextLoopback        bool
	Timeout                       time.Duration
	MaxInputBytes, MaxOutputBytes int
}
type Provider struct {
	endpoint, token         string
	client                  *http.Client
	transport               *http.Transport
	inputLimit, outputLimit int
}

func New(c Config) (*Provider, error) {
	u, err := url.Parse(c.URL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || c.Timeout < time.Second || c.Timeout > 2*time.Minute || c.MaxInputBytes < 1 || c.MaxInputBytes > 16<<20 || c.MaxOutputBytes < 1 || c.MaxOutputBytes > 2<<20 || len(c.Token) > 8192 || strings.ContainsAny(c.Token, "\r\n") {
		return nil, model.ErrInvalid
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || !c.AllowPlaintextLoopback || ip == nil || !ip.IsLoopback() {
			return nil, model.ErrInvalid
		}
	}
	var roots *x509.CertPool
	if len(c.RootCAPEM) > 0 {
		if len(c.RootCAPEM) > 1<<20 {
			return nil, model.ErrInvalid
		}
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(c.RootCAPEM) {
			return nil, model.ErrInvalid
		}
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: c.Timeout, MaxResponseHeaderBytes: 16 << 10, MaxConnsPerHost: 1, MaxIdleConns: 1, MaxIdleConnsPerHost: 1, IdleConnTimeout: 30 * time.Second, DisableCompression: true}
	client := &http.Client{Transport: transport, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Provider{u.String(), c.Token, client, transport, c.MaxInputBytes, c.MaxOutputBytes}, nil
}
func (p *Provider) Close() { p.transport.CloseIdleConnections() }
func (p *Provider) Extract(ctx context.Context, input []byte) (model.ExtractionResponse, error) {
	if len(input) == 0 || len(input) > p.inputLimit {
		return model.ExtractionResponse{}, model.ErrInvalid
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(input))
	if err != nil {
		return model.ExtractionResponse{}, model.ErrUnavailable
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Facts-Extraction-Protocol", "v1")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	response, err := p.client.Do(req)
	if err != nil {
		return model.ExtractionResponse{}, model.ErrUnavailable
	}
	defer response.Body.Close()
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK || err != nil || media != "application/json" || response.Header.Get("Content-Encoding") != "" {
		return model.ExtractionResponse{}, model.ErrUnavailable
	}
	// Read one sentinel byte beyond the approved output limit. The engine records
	// response_limit and a bounded prefix; the adapter never drains an oversized body.
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(p.outputLimit)+1))
	if err != nil {
		return model.ExtractionResponse{}, model.ErrUnavailable
	}
	identity := response.Header.Values("X-Facts-Model-Identity")
	in := response.Header.Values("X-Facts-Input-Tokens")
	out := response.Header.Values("X-Facts-Output-Tokens")
	if len(identity) != 1 || len(identity[0]) == 0 || len(identity[0]) > 256 || len(in) != 1 || len(out) != 1 {
		return model.ExtractionResponse{}, model.ErrUnavailable
	}
	inputTokens, inErr := strconv.ParseInt(in[0], 10, 64)
	outputTokens, outErr := strconv.ParseInt(out[0], 10, 64)
	if inErr != nil || outErr != nil || inputTokens < 0 || outputTokens < 0 {
		return model.ExtractionResponse{}, model.ErrUnavailable
	}
	return model.ExtractionResponse{ModelIdentity: identity[0], Body: body, InputTokens: inputTokens, OutputTokens: outputTokens}, nil
}
