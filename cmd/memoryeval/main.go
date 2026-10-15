package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/caxqueiroz/facts_engine/evaluation"
)

func read(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, evaluation.MaxInputBytes+1))
}
func run(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("memoryeval", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	dataset := flags.String("dataset", "", "dataset JSON path")
	observations := flags.String("run", "", "observations JSON path")
	if flags.Parse(args) != nil || *dataset == "" || *observations == "" || flags.NArg() != 0 {
		fmt.Fprintln(errOut, "usage: memoryeval -dataset PATH -run PATH")
		return 2
	}
	d, err := read(*dataset)
	if err != nil {
		fmt.Fprintln(errOut, "cannot read evaluation dataset")
		return 1
	}
	r, err := read(*observations)
	if err != nil {
		fmt.Fprintln(errOut, "cannot read evaluation observations")
		return 1
	}
	report, err := evaluation.Evaluate(d, r)
	if err != nil {
		fmt.Fprintln(errOut, "evaluation inputs rejected")
		return 1
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if encoder.Encode(report) != nil {
		fmt.Fprintln(errOut, "cannot write evaluation report")
		return 1
	}
	return 0
}
func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
