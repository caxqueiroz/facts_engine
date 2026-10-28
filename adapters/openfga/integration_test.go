package openfga

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/authz"
	"github.com/caxqueiroz/facts_engine/model"
	"io"
	"net/http"
	"os"
	"testing"
)

func TestLiveModel(t *testing.T) {
	endpoint := os.Getenv("TEST_OPENFGA_URL")
	if endpoint == "" {
		t.Skip("set TEST_OPENFGA_URL to a disposable OpenFGA server")
	}
	post := func(path string, body any) map[string]json.RawMessage {
		t.Helper()
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		res, err := http.Post(endpoint+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, _ := io.ReadAll(res.Body)
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			t.Fatalf("FGA %s: %d %s", path, res.StatusCode, data)
		}
		out := map[string]json.RawMessage{}
		if len(data) > 0 {
			if err := json.Unmarshal(data, &out); err != nil {
				t.Fatal(err)
			}
		}
		return out
	}
	str := func(v json.RawMessage) string {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	store := str(post("/stores", map[string]string{"name": "facts-engine-contract-test"})["id"])
	modelID := str(post("/stores/"+store+"/authorization-models", json.RawMessage(authz.ModelJSON))["authorization_model_id"])
	tuple := func(user, relation, object string) map[string]string {
		return map[string]string{"user": user, "relation": relation, "object": object}
	}
	scope := "scope:" + ObjectID("org1", "case1")
	tenant := "tenant:org1"
	alice := "user:" + ObjectID("org1", "alice")
	bob := "user:" + ObjectID("org1", "bob")
	foreign := "user:" + ObjectID("org2", "mallory")
	svc := "service:" + ObjectID("org1", "acceptor")
	tuples := []map[string]string{tuple(tenant, "tenant", scope), tuple(alice, "member", tenant), tuple(alice, "reader", scope), tuple(alice, "approver", scope), tuple(bob, "reader", scope), tuple(bob, "administrator", scope), tuple(svc, "administrator", scope), tuple(foreign, "reader", scope), tuple(svc, "member", tenant), tuple(svc, "acceptor", scope), tuple(svc, "confirmation_recorder", scope), tuple("service:"+ObjectID("org1", "orphan"), "confirmation_recorder", scope)}
	post("/stores/"+store+"/write", map[string]any{"authorization_model_id": modelID, "writes": map[string]any{"tuple_keys": tuples}})
	a, err := New(Config{URL: endpoint, StoreID: store, ModelID: modelID})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, tenant, principal, permission string
		service, want                       bool
	}{{"member reader", "org1", "alice", "can_read", false, true}, {"member approver", "org1", "alice", "can_approve", false, true}, {"reader without membership", "org1", "bob", "can_read", false, false}, {"foreign relation", "org2", "mallory", "can_read", false, false}, {"no admin escalation", "org1", "alice", "can_manage_definitions", false, false}, {"identity admin needs membership", "org1", "bob", "can_manage_identities", false, false}, {"reader cannot manage identities", "org1", "alice", "can_manage_identities", false, false}, {"identity administrator", "org1", "acceptor", "can_manage_identities", true, true}, {"delegated confirmation recorder", "org1", "acceptor", "can_record_delegated_attestation", true, true}, {"delegated recorder needs membership", "org1", "orphan", "can_record_delegated_attestation", true, false}, {"human cannot gain recorder permission", "org1", "alice", "can_record_delegated_attestation", false, false}, {"accepting service", "org1", "acceptor", "can_accept", true, true}, {"service lacks content grant", "org1", "acceptor", "can_read", true, false}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := a.Check(context.Background(), model.AuthorizationRequest{Caller: model.Caller{Tenant: c.tenant, Principal: c.principal, Service: c.service}, Scope: "case1", Permission: c.permission})
			if err != nil || d.Allowed != c.want {
				t.Fatal(d, err)
			}
		})
	}
	// A fresh check sees tuple removal even though the model ID remains unchanged.
	post("/stores/"+store+"/write", map[string]any{"authorization_model_id": modelID, "deletes": map[string]any{"tuple_keys": []map[string]string{tuple(alice, "member", tenant)}}})
	d, err := a.Check(context.Background(), model.AuthorizationRequest{Caller: model.Caller{Tenant: "org1", Principal: "alice"}, Scope: "case1", Permission: "can_read"})
	if err != nil || d.Allowed {
		t.Fatal("revocation", d, err)
	}
}
