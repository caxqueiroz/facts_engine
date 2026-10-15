package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestAttestationTombstoneAuditAndInput(t *testing.T) {
	for _, kind := range []string{"attestation", "memory_attestation"} {
		for _, malformed := range []bool{false, true} {
			a := model.AttestationTombstone(model.Attestation{ID: "attestation", Scope: "scope", Attestor: model.Caller{Tenant: "tenant"}})
			if malformed {
				a.EvidenceAuthorization = &model.AuthorizationDecision{}
			}
			payload, err := json.Marshal(a)
			if err != nil {
				t.Fatal(err)
			}
			s := &Service{}
			out, gaps, err := s.prepareAuditEntries(context.Background(), model.Caller{Tenant: "tenant"}, model.AuditExportRequest{Scope: "scope"}, []model.AuditEntry{{Kind: kind, ID: a.ID, Payload: payload}})
			if malformed {
				if !errors.Is(err, model.ErrPrecondition) {
					t.Fatal("malformed tombstone exported", err)
				}
				continue
			}
			if err != nil || len(out) != 1 || len(gaps) != 1 || gaps[0].Kind != kind+"_content" {
				t.Fatal(out, gaps, err)
			}
		}
	}
	for _, a := range []model.Attestation{{PayloadDisposed: true}, {DisposedPayloadDigest: "caller-supplied"}} {
		s := &Service{}
		if _, err := s.RecordAttestation(context.Background(), model.Caller{}, model.AttestationRequest{Attestation: a}); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("accepted caller-owned disposal metadata", err)
		}
	}
}
