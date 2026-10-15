package evaluation

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

func fixture(t *testing.T) ([]byte, []byte) {
	t.Helper()
	d, err := os.ReadFile("../examples/evaluation/dataset.json")
	if err != nil {
		t.Fatal(err)
	}
	r, err := os.ReadFile("../examples/evaluation/run.json")
	if err != nil {
		t.Fatal(err)
	}
	return d, r
}
func TestSyntheticRatesAndDeterminism(t *testing.T) {
	d, r := fixture(t)
	report, err := Evaluate(d, r)
	if err != nil {
		t.Fatal(err)
	}
	if report.Total.Cases != 8 || report.Total.FailedCases != 1 || len(report.Categories) != 8 {
		t.Fatal(report)
	}
	for _, test := range []struct {
		rate Rate
		n, d int
	}{{report.Total.UnsupportedAssertions, 1, 2}, {report.Total.MissedConflicts, 1, 1}, {report.Total.MissedAssertions, 2, 3}, {report.Total.FalseConflicts, 1, 1}} {
		if test.rate.Numerator != test.n || test.rate.Denominator != test.d || test.rate.Value == nil || *test.rate.Value != float64(test.n)/float64(test.d) {
			t.Fatal(test)
		}
	}
	for _, category := range report.Categories {
		if category.Category == "truncation" && (category.Summary.FailedCases != 1 || category.Summary.UnsupportedAssertions.Value != nil) {
			t.Fatal("failure disguised as zero error rate", category)
		}
	}
	again, err := Evaluate(d, r)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(report)
	b, _ := json.Marshal(again)
	if !bytes.Equal(a, b) {
		t.Fatal("non-deterministic report")
	}
	if bytes.Contains(a, []byte("conflict-1")) || bytes.Contains(a, []byte("request_digest")) {
		t.Fatal("case contents disclosed in aggregate report")
	}
}
func TestRejectIncompleteOrAmbiguousEvaluation(t *testing.T) {
	originalDataset, originalRun := fixture(t)
	for name, mutate := range map[string]func(*Dataset, *Run){
		"missing case":            func(_ *Dataset, r *Run) { r.Cases = r.Cases[1:] },
		"duplicate case":          func(_ *Dataset, r *Run) { r.Cases[1] = r.Cases[0] },
		"unknown case":            func(_ *Dataset, r *Run) { r.Cases[0].ID = "other" },
		"wrong binding":           func(_ *Dataset, r *Run) { r.DatasetDigest = digest([]byte("wrong")) },
		"unknown outcome":         func(_ *Dataset, r *Run) { r.Cases[0].Outcome = "skipped" },
		"missing labels":          func(d *Dataset, _ *Run) { d.Cases[0].ExpectedConflicts = nil },
		"unsupported requirement": func(d *Dataset, _ *Run) { d.Cases[0].RequiredAssertions = []string{digest([]byte("unknown"))} },
		"duplicate prediction": func(_ *Dataset, r *Run) {
			r.Cases[0].ProposedAssertions = []string{digest([]byte("x")), digest([]byte("x"))}
		},
		"failed output": func(_ *Dataset, r *Run) {
			r.Cases[0].Outcome = "failed"
			r.Cases[0].DetectedConflicts = []string{"claim"}
		},
		"missing response digest": func(_ *Dataset, r *Run) { r.Cases[0].ResponseDigest = "" },
		"missing observed labels": func(_ *Dataset, r *Run) { r.Cases[0].DetectedConflicts = nil },
		"unknown category":        func(d *Dataset, _ *Run) { d.Cases[0].Category = "private-category" },
	} {
		t.Run(name, func(t *testing.T) {
			var d Dataset
			var r Run
			if json.Unmarshal(originalDataset, &d) != nil || json.Unmarshal(originalRun, &r) != nil {
				t.Fatal("fixture")
			}
			mutate(&d, &r)
			rawD, _ := json.Marshal(d)
			if name != "wrong binding" {
				r.DatasetDigest = digest(rawD)
			}
			rawR, _ := json.Marshal(r)
			if _, err := Evaluate(rawD, rawR); !errors.Is(err, ErrInvalid) {
				t.Fatal("accepted invalid evaluation", err)
			}
		})
	}
	for _, raw := range [][]byte{
		bytes.Replace(originalDataset, []byte(`"version": "synthetic-v1"`), []byte(`"version":"x","version":"synthetic-v1"`), 1),
		bytes.Replace(originalDataset, []byte(`"version"`), []byte(`"VERSION"`), 1),
		bytes.Replace(originalDataset, []byte(`"version"`), []byte(`"extra": true, "version"`), 1),
		append(append([]byte{}, originalDataset...), []byte(`{}`)...),
		bytes.Repeat([]byte(" "), MaxInputBytes+1),
	} {
		var bound Run
		if err := json.Unmarshal(originalRun, &bound); err != nil {
			t.Fatal(err)
		}
		bound.DatasetDigest = digest(raw)
		boundJSON, err := json.Marshal(bound)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Evaluate(raw, boundJSON); !errors.Is(err, ErrInvalid) {
			t.Fatal("ambiguous JSON accepted", err)
		}
	}
}
func TestDecliningEverythingDoesNotHideMisses(t *testing.T) {
	d, raw := fixture(t)
	var r Run
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	for i := range r.Cases {
		r.Cases[i].Outcome = "completed"
		r.Cases[i].ResponseDigest = digest([]byte("declined"))
		r.Cases[i].ProposedAssertions = []string{}
		r.Cases[i].DetectedConflicts = []string{}
	}
	raw, _ = json.Marshal(r)
	report, err := Evaluate(d, raw)
	if err != nil {
		t.Fatal(err)
	}
	if report.Total.UnsupportedAssertions.Value != nil || report.Total.MissedAssertions.Numerator != 3 || report.Total.MissedConflicts.Numerator != 1 {
		t.Fatal(report.Total)
	}
}
