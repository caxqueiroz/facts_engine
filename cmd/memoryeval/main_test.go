package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/caxqueiroz/facts_engine/evaluation"
)

func TestCLIProducesReportAndSanitizesFailure(t *testing.T) {
	var out, stderr bytes.Buffer
	args := []string{"-dataset", "../../examples/evaluation/dataset.json", "-run", "../../examples/evaluation/run.json"}
	if code := run(args, &out, &stderr); code != 0 {
		t.Fatal(code, stderr.String())
	}
	var report evaluation.Report
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || report.Total.UnsupportedAssertions.Numerator != 1 {
		t.Fatal(report, err)
	}
	out.Reset()
	stderr.Reset()
	if code := run([]string{"-dataset", "PRIVATE-nonexistent-path", "-run", args[3]}, &out, &stderr); code != 1 || bytes.Contains(stderr.Bytes(), []byte("PRIVATE")) || out.Len() != 0 {
		t.Fatal(code, stderr.String())
	}
	stderr.Reset()
	if code := run(nil, &out, &stderr); code != 2 {
		t.Fatal(code)
	}
}
