package services

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestPayloadOmissionPreservesCanonicalInput(t *testing.T) {
	secret := "PRIVATE-fact-value"
	version := model.Version{ID: "version", RecordID: "record", Digest: "digest", Proposal: model.FactProposal{Command: model.Command{Purpose: "PRIVATE-purpose"}, Value: model.Value{Kind: model.Text, Text: &secret}, Sources: []model.SourceSpan{{Quote: "PRIVATE-quote"}}}, Proposer: model.Caller{Principal: "PRIVATE-proposer"}}
	original := model.MemoryVersion{ID: "memory", RecordID: "memory-record", Digest: "memory-digest", Proposal: model.MemoryProposal{Body: "PRIVATE-body", Sources: []model.SourceSpan{{Quote: "PRIVATE-memory-quote"}}}, Proposer: model.Caller{Principal: "PRIVATE-author"}, Authorization: []model.AuthorizationDecision{{ModelID: "PRIVATE-auth"}}}
	policy := &model.ExtractionPayloadRedaction{PolicyVersion: "policy", DependencyPayloads: true, OriginalMemoryPayload: true, RefreshEvidence: true}
	input := model.ExtractionInput{Schema: "memory-refresh-request-v1", Profile: model.ExtractionProfile{PayloadRedaction: policy}, Refresh: &model.MemoryRefreshInput{Original: original, Dependencies: []model.MemoryRefreshDependency{{RecordID: "record", Versions: []model.Version{version}}}, Evidence: []model.ExtractionInputMessage{{Content: "PRIVATE-evidence"}}}}
	before, _ := json.Marshal(input)
	raw, err := marshalExtractionInput(input)
	if err != nil || bytes.Contains(raw, []byte("PRIVATE")) {
		t.Fatal("private payload exported", err)
	}
	after, _ := json.Marshal(input)
	if !bytes.Equal(before, after) {
		t.Fatal("canonical input mutated")
	}
	var wire model.ExtractionInput
	if json.Unmarshal(raw, &wire) != nil || wire.PayloadRedactionVersion != "field-omit-v1" || len(wire.PayloadOmittedPaths) != 6 || wire.Refresh.Original.ID != "memory" || wire.Refresh.Dependencies[0].Versions[0].ID != "version" {
		t.Fatal("omission provenance missing")
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		t.Fatal("invalid wire JSON")
	}
	var refresh map[string]json.RawMessage
	_ = json.Unmarshal(object["Refresh"], &refresh)
	if _, exists := refresh["Evidence"]; exists {
		t.Fatal("evidence field retained")
	}
	var memory map[string]json.RawMessage
	_ = json.Unmarshal(refresh["Original"], &memory)
	for _, field := range []string{"Proposal", "Proposer", "Authorization"} {
		if _, exists := memory[field]; exists {
			t.Fatal("omitted memory field retained", field)
		}
	}
	// Omitting dependency payloads must not implicitly hide other input fields.
	input.Profile.PayloadRedaction = &model.ExtractionPayloadRedaction{PolicyVersion: "policy", DependencyPayloads: true}
	raw, err = marshalExtractionInput(input)
	if err != nil || bytes.Contains(raw, []byte("PRIVATE-fact-value")) || !bytes.Contains(raw, []byte("PRIVATE-body")) || !bytes.Contains(raw, []byte("PRIVATE-evidence")) {
		t.Fatal("incorrect omission selection", err)
	}
	input.Profile.PayloadRedaction = nil
	expected, _ := json.Marshal(input)
	raw, err = marshalExtractionInput(input)
	if err != nil || !bytes.Equal(raw, expected) {
		t.Fatal("unconfigured input encoding changed")
	}
}
func TestPayloadRedactionProfileApprovalAndPinning(t *testing.T) {
	prompt := []byte("refresh")
	policy := &model.ExtractionPayloadRedaction{PolicyVersion: "policy", DependencyPayloads: true}
	profile := model.ExtractionProfile{PayloadRedaction: policy, ID: "profile", ProviderID: "provider", RequestedModel: "model", PromptVersion: "v1", Prompt: prompt, PromptDigest: model.BytesDigest(prompt), ParserVersion: "memory-refresh-json-v1", ApplicationBuild: "test", Parameters: json.RawMessage("{}"), Timeout: time.Second, MaxInputBytes: 1000, MaxOutputBytes: 1000, MaxCandidates: 1, MaxAttempts: 1, MaxTokensPerAttempt: 100, RetryDelay: time.Second}
	options := model.ExtractionOptions{ScheduleOnly: true, Profiles: []model.ApprovedExtractor{{Profile: profile}}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 1000}
	service := &Service{redactionPolicies: map[string]bool{"policy": true}}
	if err := configureExtraction(service, options); err != nil {
		t.Fatal(err)
	}
	pinned := model.ExtractionProfileDigest(service.extractors["profile"].Profile)
	policy.RefreshEvidence = true
	if model.ExtractionProfileDigest(service.extractors["profile"].Profile) != pinned || model.ExtractionProfileDigest(profile) == pinned {
		t.Fatal("mutable or unpinned payload policy")
	}
	if err := configureExtraction(&Service{}, options); err == nil {
		t.Fatal("unapproved policy accepted")
	}
	for _, invalid := range []model.ExtractionPayloadRedaction{{PolicyVersion: "policy"}, {PolicyVersion: "unknown", DependencyPayloads: true}, {PolicyVersion: "policy", RefreshEvidence: true}} {
		options.Profiles[0].Profile.ParserVersion = "memory-extraction-json-v1"
		options.Profiles[0].Profile.PayloadRedaction = &invalid
		if err := configureExtraction(&Service{redactionPolicies: map[string]bool{"policy": true}}, options); err == nil {
			t.Fatal("invalid payload policy accepted")
		}
	}
}
