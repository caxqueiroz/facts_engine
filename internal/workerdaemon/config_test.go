package workerdaemon

import "testing"

func TestExampleAndInvalidWorkerModes(t *testing.T) {
	config, err := LoadConfig("../../examples/memoryworker.json")
	if err != nil {
		t.Fatal(err)
	}
	config.Mode = "extraction"
	if config.Validate() == nil {
		t.Fatal("unconfigured extraction mode accepted")
	}
	config.Mode = "projection"
	config.MaxAttempts = 0
	if config.Validate() == nil {
		t.Fatal("missing retry bound accepted")
	}
	config.MaxAttempts = 5
	config.DeliveryTimeoutSeconds = 0
	if config.Validate() == nil {
		t.Fatal("unbounded delivery accepted")
	}
}
