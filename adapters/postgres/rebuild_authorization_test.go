package postgres

import (
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

func TestRebuildAuthorizationBinding(t *testing.T) {
	caller := model.Caller{Tenant: "tenant", Principal: "operator", Service: true, Initiator: "human"}
	request := model.AuthorizationRequest{Caller: caller, Scope: "operations", Permission: "can_operate", Purpose: "rebuild"}
	authorization := model.AuthorizationDecision{Request: request, Allowed: true, ModelID: "model", Consistency: "HIGHER_CONSISTENCY", CheckedAt: time.Now(), ContextDigest: model.Digest(request)}
	actor := model.RebuildActor{Principal: caller.Principal, Purpose: request.Purpose, Caller: &caller, Authorization: &authorization}
	key := model.GenerationKey{Tenant: "tenant", Generation: "g1"}
	if err := validateRebuildAuthorization(key, actor); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		change func(*model.RebuildActor, *model.AuthorizationDecision)
		want   error
	}{
		{"missing caller", func(a *model.RebuildActor, _ *model.AuthorizationDecision) { a.Caller = nil }, model.ErrInvalid},
		{"missing receipt", func(a *model.RebuildActor, _ *model.AuthorizationDecision) { a.Authorization = nil }, model.ErrInvalid},
		{"different principal", func(a *model.RebuildActor, _ *model.AuthorizationDecision) { a.Principal = "other" }, model.ErrDenied},
		{"different purpose", func(a *model.RebuildActor, _ *model.AuthorizationDecision) { a.Purpose = "other" }, model.ErrDenied},
		{"altered delegation", func(_ *model.RebuildActor, d *model.AuthorizationDecision) {
			d.Request.Caller.Initiator = "other"
			d.ContextDigest = model.Digest(d.Request)
		}, model.ErrDenied},
		{"read permission", func(_ *model.RebuildActor, d *model.AuthorizationDecision) {
			d.Request.Permission = "can_read"
			d.ContextDigest = model.Digest(d.Request)
		}, model.ErrDenied},
		{"altered digest", func(_ *model.RebuildActor, d *model.AuthorizationDecision) { d.ContextDigest = "bad" }, model.ErrDenied},
		{"stale receipt", func(_ *model.RebuildActor, d *model.AuthorizationDecision) {
			d.CheckedAt = time.Now().Add(-31 * time.Second)
		}, model.ErrUnavailable},
		{"future receipt", func(_ *model.RebuildActor, d *model.AuthorizationDecision) { d.CheckedAt = time.Now().Add(time.Minute) }, model.ErrUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a, d := actor, authorization
			a.Authorization = &d
			test.change(&a, &d)
			if err := validateRebuildAuthorization(key, a); !errors.Is(err, test.want) {
				t.Fatalf("got %v, want %v", err, test.want)
			}
		})
	}
	key.Tenant = "other"
	if err := validateRebuildAuthorization(key, actor); !errors.Is(err, model.ErrDenied) {
		t.Fatalf("tenant binding: %v", err)
	}
}
