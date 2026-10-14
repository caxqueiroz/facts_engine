package services

import (
	"context"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/model"
	"testing"
	"time"
)

type budgetProvider struct{}

func (budgetProvider) Extract(context.Context, []byte) (model.ExtractionResponse, error) {
	return model.ExtractionResponse{}, model.ErrUnavailable
}
func TestExtractionCostConfiguration(t *testing.T) {
	prompt := []byte("extract")
	profile := model.ExtractionProfile{ID: "profile", ProviderID: "provider", RequestedModel: "model", PromptVersion: "v1", Prompt: prompt, PromptDigest: model.BytesDigest(prompt), ParserVersion: "facts-extraction-json-v1", ApplicationBuild: "test", Parameters: json.RawMessage(`{}`), Timeout: time.Second, MaxInputBytes: 1000, MaxOutputBytes: 1000, MaxCandidates: 1, MaxAttempts: 1, MaxTokensPerAttempt: 100, RetryDelay: time.Second, CostUnit: "test-units", MaxCostUnitsPerAttempt: 6}
	original := model.ExtractionOptions{Profiles: []model.ApprovedExtractor{{Profile: profile, Provider: budgetProvider{}}}, MaxConcurrentPerTenant: 1, DailyTokenBudget: 1000, CostUnit: "test-units", DailyCostBudgetUnits: 12}
	if err := configureExtraction(&Service{}, original); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*model.ExtractionOptions){func(o *model.ExtractionOptions) { o.CostUnit = "" }, func(o *model.ExtractionOptions) { o.DailyCostBudgetUnits = 0 }, func(o *model.ExtractionOptions) { o.DailyCostBudgetUnits = 5 }, func(o *model.ExtractionOptions) { o.CostUnit = "invalid unit" }, func(o *model.ExtractionOptions) { o.Profiles[0].Profile.CostUnit = "other" }, func(o *model.ExtractionOptions) { o.Profiles[0].Profile.MaxCostUnitsPerAttempt = 0 }, func(o *model.ExtractionOptions) { o.Profiles[0].Profile.MaxCostUnitsPerAttempt = -1 }, func(o *model.ExtractionOptions) { o.Profiles = nil }} {
		options := original
		options.Profiles = append([]model.ApprovedExtractor(nil), original.Profiles...)
		change(&options)
		if configureExtraction(&Service{}, options) == nil {
			t.Fatal("invalid cost configuration accepted", options)
		}
	}
	changed := profile
	changed.MaxCostUnitsPerAttempt++
	if model.ExtractionProfileDigest(profile) == model.ExtractionProfileDigest(changed) {
		t.Fatal("price cap not pinned to job profile")
	}
}
