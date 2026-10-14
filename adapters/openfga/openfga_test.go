package openfga

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPinnedFreshCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req["authorization_model_id"] != "model1" || req["consistency"] != "HIGHER_CONSISTENCY" {
			t.Error("unpinned check")
		}
		tuple := req["tuple_key"].(map[string]any)
		if tuple["user"] != "user:"+ObjectID("tenant1", "alice") || tuple["object"] != "scope:"+ObjectID("tenant1", "case1") {
			t.Error("unqualified identity")
		}
		w.Write([]byte(`{"allowed":true}`))
	}))
	defer server.Close()
	a, err := New(Config{URL: server.URL, StoreID: "store1", ModelID: "model1"})
	if err != nil {
		t.Fatal(err)
	}
	d, err := a.Check(context.Background(), model.AuthorizationRequest{Caller: model.Caller{Tenant: "tenant1", Principal: "alice"}, Scope: "case1", Permission: "can_read"})
	if err != nil || !d.Allowed {
		t.Fatal(d, err)
	}
}
func TestFailClosed(t *testing.T) {
	for _, body := range []string{`{}`, `invalid`, `{"allowed":false}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
		a, _ := New(Config{URL: server.URL, StoreID: "s", ModelID: "m"})
		d, err := a.Check(context.Background(), model.AuthorizationRequest{Caller: model.Caller{Tenant: "t", Principal: "p"}, Scope: "s"})
		if d.Allowed {
			t.Fatal("allowed")
		}
		if body != `{"allowed":false}` && !errors.Is(err, model.ErrUnavailable) {
			t.Fatal(err)
		}
		server.Close()
	}
}
