// Package openfga implements fresh authorization checks against a pinned model.
package openfga

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/caxqueiroz/facts_engine/model"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	URL, StoreID, ModelID, Token string
	Client                       *http.Client
}
type Authorizer struct{ config Config }

func New(c Config) (*Authorizer, error) {
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !model.ValidID(c.StoreID) || !model.ValidID(c.ModelID) {
		return nil, model.ErrInvalid
	}
	if c.Client == nil {
		c.Client = &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	return &Authorizer{c}, nil
}

// ObjectID is unambiguous even when identifiers contain punctuation.
func ObjectID(tenant, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(tenant)) + "." + base64.RawURLEncoding.EncodeToString([]byte(id))
}
func (a *Authorizer) Check(ctx context.Context, r model.AuthorizationRequest) (model.AuthorizationDecision, error) {
	d := model.AuthorizationDecision{ModelID: a.config.ModelID, Consistency: "HIGHER_CONSISTENCY", Request: r, ContextDigest: model.Digest(r)}
	if !model.ValidID(r.Caller.Tenant) || !model.ValidID(r.Caller.Principal) || !model.ValidID(r.Scope) {
		return d, model.ErrInvalid
	}
	principalType := "user"
	if r.Caller.Service {
		principalType = "service"
	}
	request := struct {
		ModelID     string `json:"authorization_model_id"`
		Consistency string `json:"consistency"`
		Tuple       struct {
			User     string `json:"user"`
			Relation string `json:"relation"`
			Object   string `json:"object"`
		} `json:"tuple_key"`
	}{ModelID: a.config.ModelID, Consistency: d.Consistency}
	request.Tuple.User = principalType + ":" + ObjectID(r.Caller.Tenant, r.Caller.Principal)
	request.Tuple.Relation = r.Permission
	request.Tuple.Object = "scope:" + ObjectID(r.Caller.Tenant, r.Scope)
	b, err := json.Marshal(request)
	if err != nil {
		return d, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(a.config.URL, "/")+"/stores/"+a.config.StoreID+"/check", bytes.NewReader(b))
	if err != nil {
		return d, err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.config.Token)
	}
	resp, err := a.config.Client.Do(req)
	if err != nil {
		return d, fmt.Errorf("authorization transport: %w", model.ErrUnavailable)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return d, model.ErrUnavailable
	}
	var result struct {
		Allowed *bool `json:"allowed"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&result); err != nil || result.Allowed == nil {
		return d, model.ErrUnavailable
	}
	d.Allowed = *result.Allowed
	d.CheckedAt = time.Now().UTC()
	return d, nil
}
