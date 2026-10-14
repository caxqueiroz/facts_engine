package runtimeconfig

import (
	"testing"
	"time"
)

func TestAttestationIdentityConfiguration(t *testing.T) {
	var disabled *AttestationIdentity
	options, closeVerifier, err := disabled.Open()
	if err != nil || options.Verifier != nil {
		t.Fatal("implicit identity verifier", err)
	}
	closeVerifier()
	config := &AttestationIdentity{URL: "https://identity.example.test/verify", Authority: "authority", TokenEnv: "TEST_AUTHORITY_TOKEN", TimeoutSeconds: 5, MaxAuthenticationAgeSeconds: 900}
	t.Setenv("TEST_AUTHORITY_TOKEN", "")
	if _, _, err := config.Open(); err == nil {
		t.Fatal("missing credential accepted")
	}
	t.Setenv("TEST_AUTHORITY_TOKEN", "test-token")
	options, closeVerifier, err = config.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer closeVerifier()
	if options.Verifier == nil || options.Authority != "authority" || options.Timeout != 5*time.Second || options.MaxAuthenticationAge != 15*time.Minute {
		t.Fatal("identity configuration not propagated")
	}
	config.RootCAFile = "missing-private-ca"
	if _, _, err := config.Open(); err == nil {
		t.Fatal("missing CA accepted")
	}
	config.TimeoutSeconds = 31
	if config.Validate() == nil {
		t.Fatal("unbounded timeout")
	}
}
