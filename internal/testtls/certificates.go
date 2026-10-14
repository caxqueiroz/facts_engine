// Package testtls creates ephemeral certificate authorities for transport tests.
package testtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"
)

type Certificate struct {
	TLS         tls.Certificate
	PEM, KeyPEM []byte
}
type Authority struct {
	Certificate *x509.Certificate
	Key         *ecdsa.PrivateKey
	PEM         []byte
	Roots       *x509.CertPool
}

func New(t *testing.T) *Authority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	encoded := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return &Authority{Certificate: certificate, Key: key, PEM: encoded, Roots: roots}
}
func (a *Authority) Issue(t *testing.T, server bool, uris []string, expires time.Time) Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial, NotBefore: time.Now().Add(-time.Hour), NotAfter: expires, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	if server {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	for _, value := range uris {
		u, err := url.Parse(value)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = append(template.URIs, u)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.Certificate, &key.PublicKey, a.Key)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	out := Certificate{PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), KeyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey})}
	out.TLS, err = tls.X509KeyPair(out.PEM, out.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
