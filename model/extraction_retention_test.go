package model

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestExtractionTombstones(t *testing.T) {
	secret := "PRIVATE-value"
	v := Version{ID: "v", RecordID: "record", Proposal: FactProposal{Key: FactKey{Scope: "dependency"}, Value: Value{Kind: Text, Text: &secret}, Sources: []SourceSpan{{EvidenceID: "source", Quote: "PRIVATE-quote"}}}}
	v.Digest = FactDigest(v.Proposal)
	j := ExtractionJob{ID: "job", Tenant: "tenant", CreatedRevision: 1, Request: ExtractionRequest{Scope: "scope"}, Profile: ExtractionProfile{ID: "profile", Prompt: []byte("PRIVATE-prompt"), Parameters: json.RawMessage(`{"secret":"PRIVATE-parameter"}`)}, Predicates: []PredicateVersion{{Predicate: "predicate", Version: "v1", Constraints: &ValueConstraints{AllowedValues: []Value{v.Proposal.Value}}}}, DependencyVersions: []Version{v}, ContentScopes: []string{"scope", "dependency"}}
	j.ProfileDigest = ExtractionProfileDigest(j.Profile)
	a := ExtractionAttempt{ID: "attempt", JobID: j.ID, Tenant: j.Tenant, Scope: "scope", RequestBytes: []byte("PRIVATE-request"), LedgerRevision: 2, Reservation: &ExtractionReservation{Tokens: 10}, OwnerNonce: "nonce"}
	a.RequestDigest = BytesDigest(a.RequestBytes)
	r := ExtractionResult{AttemptID: a.ID, JobID: j.ID, Tenant: j.Tenant, Scope: "scope", RequestDigest: a.RequestDigest, ResponseBytes: []byte("PRIVATE-response"), ResponseComplete: true, LedgerRevision: 3, Proposals: []ExtractedProposal{{RecordID: "record", VersionID: "v", Candidate: 0}}}
	r.ResponseDigest = BytesDigest(r.ResponseBytes)
	before, _ := json.Marshal([]any{j, a, r})
	jt, at, rt := ExtractionJobTombstone(j), ExtractionAttemptTombstone(a), ExtractionResultTombstone(r)
	after, _ := json.Marshal([]any{j, a, r})
	if !bytes.Equal(before, after) {
		t.Fatal("input mutated")
	}
	raw, _ := json.Marshal([]any{jt, at, rt})
	// Byte slices are base64 in JSON; inspect them explicitly too.
	if bytes.Contains(raw, []byte("PRIVATE-")) || len(jt.Profile.Prompt) > 0 || len(jt.Profile.Parameters) > 0 || len(at.RequestBytes) > 0 || len(rt.ResponseBytes) > 0 {
		t.Fatal("content retained")
	}
	if bytes.Contains(before, []byte("PayloadDisposed")) {
		t.Fatal("legacy encoding changed")
	}
	if !jt.PayloadDisposed || !at.PayloadDisposed || !rt.PayloadDisposed || jt.ProfileDigest != j.ProfileDigest || at.RequestDigest != a.RequestDigest || rt.ResponseDigest != r.ResponseDigest || !rt.ResponseComplete || !reflect.DeepEqual(rt.Proposals, r.Proposals) || !reflect.DeepEqual(at.Reservation, a.Reservation) || !reflect.DeepEqual(jt.ContentScopes, j.ContentScopes) {
		t.Fatal("provenance lost")
	}
	if jt.DependencyVersions[0].ID != v.ID || jt.DependencyVersions[0].Digest != v.Digest || jt.DependencyVersions[0].Proposal.Sources[0].EvidenceID != "source" || jt.Predicates[0].Version != "v1" {
		t.Fatal("bindings lost")
	}
	if !reflect.DeepEqual(ExtractionJobTombstone(jt), jt) || !reflect.DeepEqual(ExtractionAttemptTombstone(at), at) || !reflect.DeepEqual(ExtractionResultTombstone(rt), rt) {
		t.Fatal("not idempotent")
	}
	if ExtractionProfileDigest(jt.Profile) == j.ProfileDigest {
		t.Fatal("metadata claims original profile")
	}
}
