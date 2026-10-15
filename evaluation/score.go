// Package evaluation scores owner-labelled extraction observations offline.
// It does not infer semantic truth, invoke providers, or approve a model.
package evaluation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sort"
)

const MaxInputBytes = 16 << 20
const Schema = "facts-extraction-evaluation-v1"

var ErrInvalid = errors.New("invalid evaluation inputs")
var categories = []string{"ambiguous_subject", "assistant_claim", "contradiction", "control", "historical_date", "negation", "prompt_injection", "truncation"}
var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var modelIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$`)

type Dataset struct {
	Schema  string `json:"schema"`
	Version string `json:"version"`
	Cases   []Case `json:"cases"`
}
type Case struct {
	ID                  string   `json:"id"`
	Category            string   `json:"category"`
	SupportedAssertions []string `json:"supported_assertions"`
	RequiredAssertions  []string `json:"required_assertions"`
	ExpectedConflicts   []string `json:"expected_conflicts"`
}
type Run struct {
	Schema        string        `json:"schema"`
	DatasetDigest string        `json:"dataset_digest"`
	ProfileDigest string        `json:"profile_digest"`
	ModelIdentity string        `json:"model_identity"`
	Cases         []Observation `json:"cases"`
}
type Observation struct {
	ID                 string   `json:"id"`
	Outcome            string   `json:"outcome"`
	RequestDigest      string   `json:"request_digest"`
	ResponseDigest     string   `json:"response_digest,omitempty"`
	ProposedAssertions []string `json:"proposed_assertions"`
	DetectedConflicts  []string `json:"detected_conflicts"`
}
type Rate struct {
	Numerator   int      `json:"numerator"`
	Denominator int      `json:"denominator"`
	Value       *float64 `json:"value"`
}
type Summary struct {
	Cases                 int  `json:"cases"`
	FailedCases           int  `json:"failed_cases"`
	UnsupportedAssertions Rate `json:"unsupported_assertion_rate"`
	MissedConflicts       Rate `json:"missed_conflict_rate"`
	MissedAssertions      Rate `json:"missed_required_assertion_rate"`
	FalseConflicts        Rate `json:"false_conflict_rate"`
}
type CategorySummary struct {
	Category string  `json:"category"`
	Summary  Summary `json:"summary"`
}
type Report struct {
	Schema         string            `json:"schema"`
	DatasetVersion string            `json:"dataset_version"`
	DatasetDigest  string            `json:"dataset_digest"`
	RunDigest      string            `json:"run_digest"`
	ProfileDigest  string            `json:"profile_digest"`
	ModelIdentity  string            `json:"model_identity"`
	Total          Summary           `json:"total"`
	Categories     []CategorySummary `json:"categories"`
}

func digest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func validDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
func set(ids []string, digests bool) (map[string]bool, error) {
	if len(ids) > 256 {
		return nil, ErrInvalid
	}
	out := map[string]bool{}
	for _, id := range ids {
		if out[id] || (digests && !validDigest(id)) || (!digests && !identifier.MatchString(id)) {
			return nil, ErrInvalid
		}
		out[id] = true
	}
	return out, nil
}
func rate(n, d int) Rate {
	r := Rate{Numerator: n, Denominator: d}
	if d > 0 {
		value := float64(n) / float64(d)
		r.Value = &value
	}
	return r
}
func add(a *Summary, b Summary) {
	a.Cases += b.Cases
	a.FailedCases += b.FailedCases
	for _, pair := range [][2]*Rate{{&a.UnsupportedAssertions, &b.UnsupportedAssertions}, {&a.MissedConflicts, &b.MissedConflicts}, {&a.MissedAssertions, &b.MissedAssertions}, {&a.FalseConflicts, &b.FalseConflicts}} {
		*pair[0] = rate(pair[0].Numerator+pair[1].Numerator, pair[0].Denominator+pair[1].Denominator)
	}
}

// Evaluate requires one observation for every dataset case, including failures.
// Assertion digests and conflict IDs must be assigned under the owner's fixed
// labelling convention. Digests bind files; they do not attest their truth.
func Evaluate(datasetJSON, runJSON []byte) (Report, error) {
	var d Dataset
	var run Run
	if decode(datasetJSON, &d) != nil || decode(runJSON, &run) != nil {
		return Report{}, ErrInvalid
	}
	if d.Schema != Schema || run.Schema != Schema || !identifier.MatchString(d.Version) || run.DatasetDigest != digest(datasetJSON) || !validDigest(run.ProfileDigest) || !modelIdentity.MatchString(run.ModelIdentity) || len(d.Cases) == 0 || len(d.Cases) > 10000 || len(run.Cases) != len(d.Cases) {
		return Report{}, ErrInvalid
	}
	observations := map[string]Observation{}
	for _, o := range run.Cases {
		if !identifier.MatchString(o.ID) || !validDigest(o.RequestDigest) {
			return Report{}, ErrInvalid
		}
		if _, exists := observations[o.ID]; exists {
			return Report{}, ErrInvalid
		}
		if o.Outcome != "completed" && o.Outcome != "failed" {
			return Report{}, ErrInvalid
		}
		if o.Outcome == "completed" && (!validDigest(o.ResponseDigest) || o.ProposedAssertions == nil || o.DetectedConflicts == nil) {
			return Report{}, ErrInvalid
		}
		if o.Outcome == "failed" && (len(o.ProposedAssertions) != 0 || len(o.DetectedConflicts) != 0 || o.ResponseDigest != "" && !validDigest(o.ResponseDigest)) {
			return Report{}, ErrInvalid
		}
		observations[o.ID] = o
	}
	report := Report{Schema: Schema, DatasetVersion: d.Version, DatasetDigest: run.DatasetDigest, RunDigest: digest(runJSON), ProfileDigest: run.ProfileDigest, ModelIdentity: run.ModelIdentity}
	grouped := map[string]Summary{}
	for _, category := range categories {
		grouped[category] = Summary{}
	}
	seen := map[string]bool{}
	for _, c := range d.Cases {
		o, exists := observations[c.ID]
		if !exists || seen[c.ID] || c.SupportedAssertions == nil || c.RequiredAssertions == nil || c.ExpectedConflicts == nil {
			return Report{}, ErrInvalid
		}
		if _, known := grouped[c.Category]; !known {
			return Report{}, ErrInvalid
		}
		seen[c.ID] = true
		supported, err := set(c.SupportedAssertions, true)
		if err != nil {
			return Report{}, err
		}
		required, err := set(c.RequiredAssertions, true)
		if err != nil {
			return Report{}, err
		}
		expected, err := set(c.ExpectedConflicts, false)
		if err != nil {
			return Report{}, err
		}
		proposed, err := set(o.ProposedAssertions, true)
		if err != nil {
			return Report{}, err
		}
		detected, err := set(o.DetectedConflicts, false)
		if err != nil {
			return Report{}, err
		}
		for id := range required {
			if !supported[id] {
				return Report{}, ErrInvalid
			}
		}
		unsupported, missedAssertions, missedConflicts, falseConflicts := 0, 0, 0, 0
		for id := range proposed {
			if !supported[id] {
				unsupported++
			}
		}
		for id := range required {
			if !proposed[id] {
				missedAssertions++
			}
		}
		for id := range expected {
			if !detected[id] {
				missedConflicts++
			}
		}
		for id := range detected {
			if !expected[id] {
				falseConflicts++
			}
		}
		summary := Summary{Cases: 1, UnsupportedAssertions: rate(unsupported, len(proposed)), MissedAssertions: rate(missedAssertions, len(required)), MissedConflicts: rate(missedConflicts, len(expected)), FalseConflicts: rate(falseConflicts, len(detected))}
		if o.Outcome == "failed" {
			summary.FailedCases = 1
		}
		add(&report.Total, summary)
		group := grouped[c.Category]
		add(&group, summary)
		grouped[c.Category] = group
	}
	for category, summary := range grouped {
		report.Categories = append(report.Categories, CategorySummary{Category: category, Summary: summary})
	}
	sort.Slice(report.Categories, func(i, j int) bool { return report.Categories[i].Category < report.Categories[j].Category })
	return report, nil
}

// Reject duplicate/ambiguous keys and excessive nesting before typed decoding.
func decode(raw []byte, target any) error {
	if len(raw) == 0 || len(raw) > MaxInputBytes {
		return ErrInvalid
	}
	scanner := json.NewDecoder(bytes.NewReader(raw))
	var value func(int) error
	value = func(depth int) error {
		if depth > 32 {
			return ErrInvalid
		}
		token, err := scanner.Token()
		if err != nil {
			return ErrInvalid
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for scanner.More() {
				token, err := scanner.Token()
				if err != nil {
					return ErrInvalid
				}
				key, ok := token.(string)
				if !ok || len(key) == 0 || len(key) > 64 || keys[key] {
					return ErrInvalid
				}
				for _, c := range key {
					if (c < 'a' || c > 'z') && c != '_' {
						return ErrInvalid
					}
				}
				keys[key] = true
				if err := value(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for scanner.More() {
				if err := value(depth + 1); err != nil {
					return err
				}
			}
		default:
			return ErrInvalid
		}
		_, err = scanner.Token()
		return err
	}
	if value(0) != nil {
		return ErrInvalid
	}
	if _, err := scanner.Token(); err != io.EOF {
		return ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return ErrInvalid
	}
	return nil
}
