package runtimeconfig

import "github.com/caxqueiroz/facts_engine/model"

// ExtractionScheduling contains approved profiles, never provider implementations
// or credentials. The engine performs full profile validation before serving.
type ExtractionScheduling struct {
	Profiles                []model.ExtractionProfile `json:"profiles"`
	RedactionPolicyVersions []string                  `json:"redaction_policy_versions,omitempty"`
	MaxConcurrentPerTenant  int                       `json:"max_concurrent_per_tenant"`
	DailyTokenBudget        int64                     `json:"daily_token_budget"`
	CostUnit                string                    `json:"cost_unit,omitempty"`
	DailyCostBudgetUnits    int64                     `json:"daily_cost_budget_units,omitempty"`
}

func (c *ExtractionScheduling) Validate() error {
	if c == nil || len(c.Profiles) < 1 || len(c.Profiles) > 32 || len(c.RedactionPolicyVersions) > 64 || c.MaxConcurrentPerTenant < 1 || c.MaxConcurrentPerTenant > 32 || c.DailyTokenBudget < 1 {
		return model.ErrInvalid
	}
	return nil
}
func (c *ExtractionScheduling) Options() model.ExtractionOptions {
	o := model.ExtractionOptions{ScheduleOnly: true, MaxConcurrentPerTenant: c.MaxConcurrentPerTenant, DailyTokenBudget: c.DailyTokenBudget, CostUnit: c.CostUnit, DailyCostBudgetUnits: c.DailyCostBudgetUnits}
	for _, p := range c.Profiles {
		o.Profiles = append(o.Profiles, model.ApprovedExtractor{Profile: p})
	}
	return o
}
