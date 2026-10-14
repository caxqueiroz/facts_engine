package workerdaemon

import (
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
)

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

func TestAuditWorkerConfiguration(t *testing.T) {
	config, err := LoadConfig("../../examples/memoryworker-audit.json")
	if err != nil {
		t.Fatal(err)
	}
	config.Audit.KeyID = ""
	if config.Validate() == nil {
		t.Fatal("missing signing key identity accepted")
	}
	config.Audit.KeyID = "key"
	config.Projection.Tenant = "other"
	if config.Validate() == nil {
		t.Fatal("mixed worker modes accepted")
	}
}

func TestArchiveWorkerTrustConfiguration(t *testing.T) {
	config, err := LoadConfig("../../examples/memoryworker-archive.json")
	if err != nil {
		t.Fatal(err)
	}
	config.Archive.Directory = "relative/archive"
	if config.Validate() == nil {
		t.Fatal("relative archive accepted")
	}
	config.Archive.Directory = "/archive"
	config.Archive.KeyID = "unknown"
	if config.Validate() == nil {
		t.Fatal("missing current signing trust accepted")
	}
	config.Archive.KeyID = "archive-development-v1"
	config.Archive.TrustedKeys[config.Archive.KeyID] = "invalid"
	if config.Validate() == nil {
		t.Fatal("malformed public key accepted")
	}
	config.Archive = nil
	if config.Validate() == nil {
		t.Fatal("missing archive config accepted")
	}
}

type configLedger struct{ model.Ledger }
type configAuth struct{ model.Authorizer }
type configEvidence struct{ model.EvidenceStore }

func TestRefreshWorkerConfiguration(t *testing.T) {
	config, err := LoadConfig("../../examples/memoryworker-refresh.json")
	if err != nil {
		t.Fatal(err)
	}
	options := config.Refresh.options()
	if _, err := facts.NewWithOptions(configLedger{}, configAuth{}, configEvidence{}, model.EngineOptions{Extraction: options}); err != nil {
		t.Fatal("example profile rejected by engine", err)
	}
	if !options.ScheduleOnly || len(options.Profiles) != 1 || options.Profiles[0].Provider != nil {
		t.Fatal("refresh enables provider execution")
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Refresh = nil },
		func(c *Config) { c.Refresh.Bindings[0].ProfileID = "unknown" },
		func(c *Config) { c.Refresh.Bindings = append(c.Refresh.Bindings, c.Refresh.Bindings[0]) },
		func(c *Config) { c.Refresh.Profiles[0].ParserVersion = "facts-extraction-json-v1" },
		func(c *Config) { c.Refresh.Profiles = append(c.Refresh.Profiles, c.Refresh.Profiles[0]) },
		func(c *Config) { c.Refresh.Purpose = "" },
		func(c *Config) { c.Refresh.DailyTokenBudget = 0 },
		func(c *Config) {
			c.Mode = "audit"
			c.Audit = AuditConfig{Tenant: "tenant", Stream: "stream", KeyID: "key"}
		},
		func(c *Config) { c.Projection.Tenant = "tenant" },
	} {
		c, err := LoadConfig("../../examples/memoryworker-refresh.json")
		if err != nil {
			t.Fatal(err)
		}
		mutate(&c)
		if c.Validate() == nil {
			t.Fatal("invalid or mixed refresh configuration accepted")
		}
	}
}

func TestRefreshSweepConfiguration(t *testing.T) {
	c, err := LoadConfig("../../examples/memoryworker-refresh-sweep.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, interval := range []int{0, -1, 1441} {
		c.Refresh.SweepIntervalMinutes = interval
		if c.Validate() == nil {
			t.Fatal("invalid interval accepted", interval)
		}
	}
	c.Refresh.SweepIntervalMinutes = 60
	c.MaxAttempts = 1
	if c.Validate() == nil {
		t.Fatal("sweep silently accepted queue settings")
	}
	c.MaxAttempts = 0
	c.Mode = "refresh"
	if c.Validate() == nil {
		t.Fatal("consumer silently accepted ticker configuration")
	}
}

func TestExtractionWorkerConfiguration(t *testing.T) {
	c, err := LoadConfig("../../examples/memoryworker-extraction.json")
	if err != nil {
		t.Fatal(err)
	}
	if c.Extraction == nil {
		t.Fatal("missing execution config")
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Extraction = nil },
		func(c *Config) { c.Extraction.Scheduling = nil },
		func(c *Config) { c.Extraction.Gateways = nil },
		func(c *Config) { c.Extraction.Gateways[0].ProfileID = "unknown" },
		func(c *Config) { c.Extraction.Gateways[0].TokenEnv = "invalid name" },
		func(c *Config) {
			c.Extraction.Scheduling.Profiles = append(c.Extraction.Scheduling.Profiles, c.Extraction.Scheduling.Profiles[0])
			c.Extraction.Gateways = append(c.Extraction.Gateways, c.Extraction.Gateways[0])
		},
		func(c *Config) { c.Mode = "refresh" },
	} {
		copy, err := LoadConfig("../../examples/memoryworker-extraction.json")
		if err != nil {
			t.Fatal(err)
		}
		mutate(&copy)
		if copy.Validate() == nil {
			t.Fatal("invalid extraction configuration accepted")
		}
	}
	t.Setenv("MEMORY_EXTRACTION_GATEWAY_TOKEN", "")
	if _, close, err := openExtractionGateways(c.Extraction); err == nil {
		close()
		t.Fatal("missing configured credential accepted")
	}
	t.Setenv("MEMORY_EXTRACTION_GATEWAY_TOKEN", "test-secret")
	options, close, err := openExtractionGateways(c.Extraction)
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	if options.ScheduleOnly || options.Profiles[0].Provider == nil {
		t.Fatal("executor lacks provider")
	}
	if _, err := facts.NewWithOptions(configLedger{}, configAuth{}, configEvidence{}, model.EngineOptions{Extraction: options}); err != nil {
		t.Fatal("invalid example profiles", err)
	}
}
