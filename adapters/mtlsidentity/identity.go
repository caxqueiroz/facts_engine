// Package mtlsidentity binds verified client certificates to host-configured
// service identities. It does not interpret gateway or user identity headers.
package mtlsidentity

import (
	"context"
	"crypto/x509"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"net/http"
	"net/url"
	"time"
)

type Binding struct{ URI, Tenant, Principal string }
type Authenticator struct{ bindings map[string]model.Caller }
type callerKey struct{ authenticator *Authenticator }

func New(bindings []Binding) (*Authenticator, error) {
	if len(bindings) == 0 || len(bindings) > 1024 {
		return nil, model.ErrInvalid
	}
	a := &Authenticator{bindings: make(map[string]model.Caller, len(bindings))}
	for _, b := range bindings {
		u, err := url.Parse(b.URI)
		if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.String() != b.URI || !model.ValidID(b.Tenant) || !model.ValidID(b.Principal) || len(b.URI) > 2048 {
			return nil, model.ErrInvalid
		}
		if _, exists := a.bindings[b.URI]; exists {
			return nil, model.ErrInvalid
		}
		a.bindings[b.URI] = model.Caller{Tenant: b.Tenant, Principal: b.Principal, Service: true}
	}
	return a, nil
}

// Wrap must run on the TLS-terminating server. The server must use
// tls.RequireAndVerifyClientCert with the host's approved client roots.
// Certificate validity is rechecked per request, including reused connections.
func (a *Authenticator) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, err := a.authenticate(r)
		if err != nil {
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), callerKey{a}, caller)))
	})
}
func (a *Authenticator) authenticate(r *http.Request) (model.Caller, error) {
	denied := errors.New("unverified workload identity")
	if r.TLS == nil || !r.TLS.HandshakeComplete || len(r.TLS.PeerCertificates) == 0 || len(r.TLS.VerifiedChains) == 0 {
		return model.Caller{}, denied
	}
	leaf := r.TLS.PeerCertificates[0]
	if len(leaf.URIs) != 1 {
		return model.Caller{}, denied
	}
	now := time.Now()
	validChain := false
	for _, chain := range r.TLS.VerifiedChains {
		if len(chain) == 0 || !chain[0].Equal(leaf) {
			continue
		}
		valid := true
		for _, cert := range chain {
			if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
				valid = false
				break
			}
		}
		if valid {
			validChain = true
			break
		}
	}
	if !validChain {
		return model.Caller{}, denied
	}
	// Verification normally enforces EKU; retaining the check also prevents a
	// server-only certificate from being treated as a workload by custom hosts.
	clientUsage := len(leaf.ExtKeyUsage) == 0
	for _, usage := range leaf.ExtKeyUsage {
		if usage == x509.ExtKeyUsageClientAuth || usage == x509.ExtKeyUsageAny {
			clientUsage = true
		}
	}
	if !clientUsage {
		return model.Caller{}, denied
	}
	caller, ok := a.bindings[leaf.URIs[0].String()]
	if !ok {
		return model.Caller{}, denied
	}
	return caller, nil
}

// Verify is suitable for connectapi.IdentityVerifier after Wrap has established
// the request identity. Calling it without that middleware fails closed.
func (a *Authenticator) Verify(ctx context.Context) (model.Caller, error) {
	caller, ok := ctx.Value(callerKey{a}).(model.Caller)
	if !ok {
		return model.Caller{}, model.ErrDenied
	}
	return caller, nil
}
