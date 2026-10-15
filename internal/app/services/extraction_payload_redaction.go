package services

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

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
	hadFieldOmission := len(paths) > 0
	if len(input.PayloadRanges) > 0 {
		if err := validatePayloadRanges(input.PayloadRanges, policy); err != nil {
			return nil, err
		}
		ranges := append([]model.ExtractionPayloadRange(nil), input.PayloadRanges...)
		sort.Slice(ranges, func(i, j int) bool {
			if ranges[i].Path != ranges[j].Path {
				return ranges[i].Path < ranges[j].Path
			}
			return ranges[i].Start > ranges[j].Start
		})
		for _, r := range ranges {
			parts := strings.Split(strings.TrimPrefix(r.Path, "/"), "/")
			wireRaw, err := redactJSONPointer(wire, parts, r)
			if err != nil {
				return nil, err
			}
			if err = json.Unmarshal(wireRaw, &wire); err != nil {
				return nil, err
			}
			paths = append(paths, fmt.Sprintf("%s#%d:%d", r.Path, r.Start, r.End))
		}
	}
	version := "field-omit-v1"
	if len(input.PayloadRanges) > 0 && hadFieldOmission {
		version = "field-omit-and-range-v1"
	} else if len(input.PayloadRanges) > 0 {
		version = "field-range-omit-v1"
	}
	wire["PayloadRedactionVersion"], err = json.Marshal(version)
	if err != nil {
		return nil, err
	}
	wire["PayloadOmittedPaths"], err = json.Marshal(paths)
	if err != nil {
		return nil, err
	}
	return json.Marshal(wire)
}

func validatePayloadRanges(ranges []model.ExtractionPayloadRange, policy *model.ExtractionPayloadRedaction) error {
	if policy == nil || len(ranges) == 0 || len(ranges) > 256 {
		return model.ErrInvalid
	}
	ordered := append([]model.ExtractionPayloadRange(nil), ranges...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Path != ordered[j].Path {
			return ordered[i].Path < ordered[j].Path
		}
		return ordered[i].Start < ordered[j].Start
	})
	for i, r := range ordered {
		if r.Start < 0 || r.End <= r.Start || !payloadPathAllowed(r.Path, policy) {
			return model.ErrInvalid
		}
		if i > 0 && ordered[i-1].Path == r.Path && ordered[i-1].End > r.Start {
			return model.ErrInvalid
		}
	}
	return nil
}

func payloadPathAllowed(path string, p *model.ExtractionPayloadRedaction) bool {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") {
		return false
	}
	index := func(s string) bool { n, err := strconv.Atoi(s); return err == nil && n >= 0 && strconv.Itoa(n) == s }
	if len(parts) == 5 && parts[0] == "DependencyVersions" && index(parts[1]) && parts[2] == "Proposal" && parts[3] == "Value" && parts[4] == "Text" {
		return p.DependencyPayloadRanges
	}
	if len(parts) == 6 && parts[0] == "DependencyVersions" && index(parts[1]) && parts[2] == "Proposal" && parts[3] == "Sources" && index(parts[4]) && parts[5] == "Quote" {
		return p.DependencyPayloadRanges
	}
	if len(parts) == 8 && parts[0] == "Refresh" && parts[1] == "Dependencies" && index(parts[2]) && parts[3] == "Versions" && index(parts[4]) && parts[5] == "Proposal" && parts[6] == "Value" && parts[7] == "Text" {
		return p.DependencyPayloadRanges
	}
	if len(parts) == 9 && parts[0] == "Refresh" && parts[1] == "Dependencies" && index(parts[2]) && parts[3] == "Versions" && index(parts[4]) && parts[5] == "Proposal" && parts[6] == "Sources" && index(parts[7]) && parts[8] == "Quote" {
		return p.DependencyPayloadRanges
	}
	if len(parts) == 6 && parts[0] == "Refresh" && parts[1] == "Original" && parts[2] == "Proposal" && parts[3] == "Sources" && index(parts[4]) && parts[5] == "Quote" {
		return p.OriginalMemoryPayloadRanges
	}
	if len(parts) == 4 && parts[0] == "Refresh" && parts[1] == "Original" && parts[2] == "Proposal" && parts[3] == "Body" {
		return p.OriginalMemoryPayloadRanges
	}
	if len(parts) == 4 && parts[0] == "Refresh" && parts[1] == "Evidence" && index(parts[2]) && parts[3] == "Content" {
		return p.RefreshEvidenceRanges
	}
	return false
}

func redactJSONPointer(root map[string]json.RawMessage, parts []string, r model.ExtractionPayloadRange) (json.RawMessage, error) {
	var update func(json.RawMessage, int) (json.RawMessage, error)
	update = func(raw json.RawMessage, at int) (json.RawMessage, error) {
		if at == len(parts) {
			var value string
			if err := json.Unmarshal(raw, &value); err != nil || r.End > len(value) || !utf8.ValidString(value) || (r.Start > 0 && !utf8.RuneStart(value[r.Start])) || (r.End < len(value) && !utf8.RuneStart(value[r.End])) {
				return nil, model.ErrInvalid
			}
			return json.Marshal(value[:r.Start] + "[REDACTED]" + value[r.End:])
		}
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) == nil && object != nil {
			child, ok := object[parts[at]]
			if !ok {
				return nil, model.ErrInvalid
			}
			updated, err := update(child, at+1)
			if err != nil {
				return nil, err
			}
			object[parts[at]] = updated
			return json.Marshal(object)
		}
		var array []json.RawMessage
		if json.Unmarshal(raw, &array) != nil {
			return nil, model.ErrInvalid
		}
		idx, err := strconv.Atoi(parts[at])
		if err != nil || idx < 0 || idx >= len(array) || strconv.Itoa(idx) != parts[at] {
			return nil, model.ErrInvalid
		}
		updated, err := update(array[idx], at+1)
		if err != nil {
			return nil, err
		}
		array[idx] = updated
		return json.Marshal(array)
	}
	raw, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	return update(raw, 0)
}
