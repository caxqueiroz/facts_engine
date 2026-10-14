package runtimeconfig

import (
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
)

type schedulingLedger struct{ model.Ledger }
type schedulingAuth struct{ model.Authorizer }
type schedulingEvidence struct{ model.EvidenceStore }

func TestSchedulingConfiguration(t *testing.T) {
	// The full service example is checked by daemon tests; read its scheduling
	// section through the same strict parser with the complete known shape there.
	profile := model.ExtractionProfile{ID: "profile"}
	c := ExtractionScheduling{Profiles: []model.ExtractionProfile{profile}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 1000}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	o := c.Options()
	if !o.ScheduleOnly || len(o.Profiles) != 1 || o.Profiles[0].Provider != nil {
		t.Fatal("scheduling config enabled provider execution")
	}
	if _, err := facts.NewWithOptions(schedulingLedger{}, schedulingAuth{}, schedulingEvidence{}, model.EngineOptions{Extraction: o}); err == nil {
		t.Fatal("incomplete profile passed full engine validation")
	}
	c.Profiles = nil
	if c.Validate() == nil {
		t.Fatal("empty profiles accepted")
	}
	c.Profiles = make([]model.ExtractionProfile, 33)
	if c.Validate() == nil {
		t.Fatal("unbounded profiles accepted")
	}
	c.Profiles = []model.ExtractionProfile{profile}
	c.DailyTokenBudget = 0
	if c.Validate() == nil {
		t.Fatal("missing budget accepted")
	}
}
