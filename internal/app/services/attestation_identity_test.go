package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

type unusedIdentityVerifier struct{}

func (unusedIdentityVerifier) VerifyAttestationIdentity(context.Context, model.AttestationIdentityRequest) (model.AttestationIdentityReceipt, error) {
	return model.AttestationIdentityReceipt{}, model.ErrUnavailable
}
func TestIdentityReceiptBinding(t *testing.T) {
	s := &Service{}
	if err := configureAttestationIdentity(s, model.AttestationIdentityOptions{Verifier: unusedIdentityVerifier{}, Authority: "idp"}); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	request := model.AttestationIdentityRequest{Recorder: model.Caller{Tenant: "tenant", Principal: "recorder", Service: true}, Request: model.DelegatedAttestationRequest{GrantID: "grant", AttestationRequest: model.AttestationRequest{Command: model.Command{Key: "command", Purpose: "confirmation"}, Attestation: model.Attestation{ID: "att", Scope: "scope", RecordID: "record", VersionID: "version", ContentDigest: "digest", Confirmation: &model.ClientConfirmation{RespondedAt: at.Add(-time.Second)}, Attestor: model.Caller{Tenant: "tenant", Principal: "client"}}}}}
	valid := model.AttestationIdentityReceipt{Authority: "idp", BindingID: "binding", RequestDigest: model.AttestationIdentityDigest(request), Attestor: request.Request.Attestation.Attestor, AuthenticatedAt: at.Add(-time.Minute), VerifiedAt: at, ExpiresAt: at.Add(time.Minute)}
	if !s.validIdentityReceipt(valid, request, at) {
		t.Fatal("valid receipt rejected")
	}
	for _, change := range []func(*model.AttestationIdentityReceipt){
		func(r *model.AttestationIdentityReceipt) { r.Authority = "other" }, func(r *model.AttestationIdentityReceipt) { r.BindingID = "" }, func(r *model.AttestationIdentityReceipt) { r.RequestDigest = "wrong" }, func(r *model.AttestationIdentityReceipt) { r.Attestor.Tenant = "other" }, func(r *model.AttestationIdentityReceipt) { r.Attestor.Principal = "other" }, func(r *model.AttestationIdentityReceipt) { r.Attestor.Service = true }, func(r *model.AttestationIdentityReceipt) { r.VerifiedAt = at.Add(-31 * time.Second) }, func(r *model.AttestationIdentityReceipt) { r.VerifiedAt = at.Add(2 * time.Second) }, func(r *model.AttestationIdentityReceipt) { r.AuthenticatedAt = at.Add(-time.Hour) }, func(r *model.AttestationIdentityReceipt) { r.AuthenticatedAt = at.Add(time.Second) }, func(r *model.AttestationIdentityReceipt) { r.ExpiresAt = at }, func(r *model.AttestationIdentityReceipt) { r.ExpiresAt = at.Add(6 * time.Minute) },
	} {
		r := valid
		change(&r)
		if s.validIdentityReceipt(r, request, at) {
			t.Fatal("unbound identity receipt", r)
		}
	}
	changed := request
	changed.Request.Key = "other-command"
	if s.validIdentityReceipt(valid, changed, at) {
		t.Fatal("command replay")
	}
	changed = request
	changed.Request.Attestation.VersionID = "replacement"
	if s.validIdentityReceipt(valid, changed, at) {
		t.Fatal("version replay")
	}
	for _, options := range []model.AttestationIdentityOptions{{Authority: "idp"}, {Verifier: unusedIdentityVerifier{}}, {Verifier: unusedIdentityVerifier{}, Authority: "idp", Timeout: time.Hour}, {Verifier: unusedIdentityVerifier{}, Authority: "idp", MaxAuthenticationAge: 48 * time.Hour}} {
		if configureAttestationIdentity(&Service{}, options) == nil {
			t.Fatal("unsafe identity options", options)
		}
	}
}
