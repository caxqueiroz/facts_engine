package runtimeconfig

import (
	"errors"
	"io"
	"os"
	"time"

	"github.com/caxqueiroz/facts_engine/adapters/attestationhttp"
	"github.com/caxqueiroz/facts_engine/model"
)

type AttestationIdentity struct {
	URL                         string `json:"url"`
	Authority                   string `json:"authority"`
	TokenEnv                    string `json:"token_env"`
	RootCAFile                  string `json:"root_ca_file,omitempty"`
	TimeoutSeconds              int    `json:"timeout_seconds"`
	MaxAuthenticationAgeSeconds int    `json:"max_authentication_age_seconds"`
}

func (c *AttestationIdentity) Validate() error {
	if c == nil {
		return nil
	}
	if c.TimeoutSeconds < 1 || c.TimeoutSeconds > 30 || !envName.MatchString(c.TokenEnv) || c.MaxAuthenticationAgeSeconds < 60 || c.MaxAuthenticationAgeSeconds > 86400 {
		return model.ErrInvalid
	}
	return (attestationhttp.Config{URL: c.URL, Authority: c.Authority, Token: "configuration-validation", Timeout: time.Duration(c.TimeoutSeconds) * time.Second}).Validate()
}
func (c *AttestationIdentity) Open() (model.AttestationIdentityOptions, func(), error) {
	options := model.AttestationIdentityOptions{}
	if c == nil {
		return options, func() {}, nil
	}
	if c.Validate() != nil {
		return options, nil, errors.New("invalid attestation identity configuration")
	}
	config := attestationhttp.Config{URL: c.URL, Authority: c.Authority, Token: os.Getenv(c.TokenEnv), Timeout: time.Duration(c.TimeoutSeconds) * time.Second}
	if c.RootCAFile != "" {
		file, err := os.Open(c.RootCAFile)
		if err != nil {
			return options, nil, errors.New("cannot open identity authority CA")
		}
		raw, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		_ = file.Close()
		if err != nil || len(raw) == 0 || len(raw) > 1<<20 {
			return options, nil, errors.New("invalid identity authority CA")
		}
		config.RootCAPEM = raw
	}
	verifier, err := attestationhttp.New(config)
	if err != nil {
		return options, nil, errors.New("cannot initialize identity authority")
	}
	options = model.AttestationIdentityOptions{Verifier: verifier, Authority: c.Authority, Timeout: config.Timeout, MaxAuthenticationAge: time.Duration(c.MaxAuthenticationAgeSeconds) * time.Second}
	return options, verifier.Close, nil
}
