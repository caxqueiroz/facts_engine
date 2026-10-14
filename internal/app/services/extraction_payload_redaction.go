package services

import (
	"encoding/json"
	"fmt"

	"github.com/caxqueiroz/facts_engine/model"
)

// Serialize a separate wire view: proposal construction and eligibility checks
// continue to use the original immutable input. No canonical payload is mutated.
func marshalExtractionInput(input model.ExtractionInput) ([]byte, error) {
	raw, err := json.Marshal(input)
	if err != nil || input.Profile.PayloadRedaction == nil {
		return raw, err
	}
	policy := input.Profile.PayloadRedaction
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	paths := []string{}
	redactVersion := func(raw json.RawMessage, path string) (json.RawMessage, error) {
		var version map[string]json.RawMessage
		if err := json.Unmarshal(raw, &version); err != nil {
			return nil, err
		}
		for _, field := range []string{"Proposal", "Proposer", "Authorization"} {
			if _, exists := version[field]; exists {
				delete(version, field)
				paths = append(paths, path+"/"+field)
			}
		}
		return json.Marshal(version)
	}
	redactVersions := func(raw json.RawMessage, path string) (json.RawMessage, error) {
		var versions []json.RawMessage
		if err := json.Unmarshal(raw, &versions); err != nil {
			return nil, err
		}
		for i := range versions {
			versions[i], err = redactVersion(versions[i], fmt.Sprintf("%s/%d", path, i))
			if err != nil {
				return nil, err
			}
		}
		return json.Marshal(versions)
	}
	if policy.DependencyPayloads {
		if raw, exists := wire["DependencyVersions"]; exists {
			wire["DependencyVersions"], err = redactVersions(raw, "/DependencyVersions")
			if err != nil {
				return nil, err
			}
		}
	}
	if raw, exists := wire["Refresh"]; exists {
		var refresh map[string]json.RawMessage
		if err := json.Unmarshal(raw, &refresh); err != nil {
			return nil, err
		}
		if policy.OriginalMemoryPayload {
			refresh["Original"], err = redactVersion(refresh["Original"], "/Refresh/Original")
			if err != nil {
				return nil, err
			}
		}
		if policy.RefreshEvidence {
			if _, exists := refresh["Evidence"]; exists {
				delete(refresh, "Evidence")
				paths = append(paths, "/Refresh/Evidence")
			}
		}
		if policy.DependencyPayloads {
			var dependencies []map[string]json.RawMessage
			if err := json.Unmarshal(refresh["Dependencies"], &dependencies); err != nil {
				return nil, err
			}
			for i, dependency := range dependencies {
				dependency["Versions"], err = redactVersions(dependency["Versions"], fmt.Sprintf("/Refresh/Dependencies/%d/Versions", i))
				if err != nil {
					return nil, err
				}
			}
			refresh["Dependencies"], err = json.Marshal(dependencies)
			if err != nil {
				return nil, err
			}
		}
		wire["Refresh"], err = json.Marshal(refresh)
		if err != nil {
			return nil, err
		}
	}
	wire["PayloadRedactionVersion"], err = json.Marshal("field-omit-v1")
	if err != nil {
		return nil, err
	}
	wire["PayloadOmittedPaths"], err = json.Marshal(paths)
	if err != nil {
		return nil, err
	}
	return json.Marshal(wire)
}
