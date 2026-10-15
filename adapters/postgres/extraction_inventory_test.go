package postgres

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestExtractionInputInventory(t *testing.T) {
	version := func(id string, proposal bool) map[string]any {
		v := map[string]any{"ID": id, "RecordID": "record"}
		if proposal {
			v["Proposal"] = map[string]any{"Value": "private"}
		}
		return v
	}
	wire := map[string]any{"Schema": "memory-refresh-request-v1", "Tenant": "tenant", "JobID": "job", "AttemptID": "attempt", "Profile": model.ExtractionProfile{}, "Refresh": map[string]any{"Original": version("memory", true), "Dependencies": []any{map[string]any{"RecordID": "record", "Versions": []any{version("b", true), version("a", true), version("a", true)}}}}}
	attempt := func(w map[string]any) model.ExtractionAttempt {
		b, err := json.Marshal(w)
		if err != nil {
			t.Fatal(err)
		}
		return model.ExtractionAttempt{ID: "attempt", Tenant: "tenant", JobID: "job", RequestBytes: b, RequestDigest: model.BytesDigest(b)}
	}
	a := attempt(wire)
	facts, memories, err := extractionInputReferences(a)
	if err != nil || !reflect.DeepEqual(facts, []extractionInputReference{{"record", "a", true}, {"record", "b", true}}) || !reflect.DeepEqual(memories, []extractionInputReference{{"record", "memory", true}}) {
		t.Fatal(facts, memories, err)
	}
	for _, change := range []func(map[string]any){
		func(w map[string]any) { w["Tenant"] = "other" },
		func(w map[string]any) { w["JobID"] = "other" },
		func(w map[string]any) { w["AttemptID"] = "other" },
		func(w map[string]any) { w["Schema"] = "unknown" },
		func(w map[string]any) { w["Schema"] = "facts-extraction-input-v1" },
		func(w map[string]any) {
			w["Refresh"].(map[string]any)["Original"].(map[string]any)["PayloadDisposed"] = true
		},
		func(w map[string]any) { delete(w["Refresh"].(map[string]any)["Original"].(map[string]any), "Proposal") },
		func(w map[string]any) { w["Refresh"].(map[string]any)["Original"].(map[string]any)["Proposal"] = nil },
		func(w map[string]any) {
			w["Refresh"].(map[string]any)["Dependencies"].([]any)[0].(map[string]any)["RecordID"] = "other"
		},
	} {
		var w map[string]any
		_ = json.Unmarshal(a.RequestBytes, &w)
		change(w)
		if _, _, err := extractionInputReferences(attempt(w)); !errors.Is(err, model.ErrInvalid) {
			t.Fatal("invalid request inventoried", err)
		}
	}
	bad := a
	bad.RequestDigest = "wrong"
	if _, _, err := extractionInputReferences(bad); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("digest not checked", err)
	}
	wire["Profile"] = model.ExtractionProfile{PayloadRedaction: &model.ExtractionPayloadRedaction{PolicyVersion: "approved", DependencyPayloads: true, OriginalMemoryPayload: true}}
	wire["PayloadRedactionVersion"] = "field-omit-v1"
	refresh := wire["Refresh"].(map[string]any)
	refresh["Original"] = version("memory", false)
	refresh["Dependencies"] = []any{map[string]any{"RecordID": "record", "Versions": []any{version("a", false), version("a", true), version("b", false)}}}
	facts, memories, err = extractionInputReferences(attempt(wire))
	if err != nil || !reflect.DeepEqual(facts, []extractionInputReference{{"record", "a", true}, {"record", "b", false}}) || !reflect.DeepEqual(memories, []extractionInputReference{{"record", "memory", false}}) {
		t.Fatal("redaction/deduplication inventory", facts, memories, err)
	}
	wire["Schema"] = "memory-extraction-input-v1"
	delete(wire, "Refresh")
	wire["DependencyVersions"] = []any{version("a", false)}
	facts, memories, err = extractionInputReferences(attempt(wire))
	if err != nil || len(facts) != 1 || facts[0].PayloadRetained || len(memories) != 0 {
		t.Fatal(facts, memories, err)
	}
	delete(wire, "DependencyVersions")
	wire["Schema"] = "facts-extraction-input-v1"
	facts, memories, err = extractionInputReferences(attempt(wire))
	if err != nil || len(facts) != 0 || len(memories) != 0 {
		t.Fatal("zero-reference inventory failed", facts, memories, err)
	}
}
