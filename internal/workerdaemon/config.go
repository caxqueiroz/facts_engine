package workerdaemon

import (
	"encoding/hex"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"path/filepath"
)

type AuditConfig struct {
	Tenant string `json:"tenant"`
	Stream string `json:"stream"`
	KeyID  string `json:"key_id"`
}
type ArchiveConfig struct {
	Tenant      string            `json:"tenant"`
	Stream      string            `json:"stream"`
	Directory   string            `json:"directory"`
	KeyID       string            `json:"key_id"`
	TrustedKeys map[string]string `json:"trusted_keys"`
}

func (c *ArchiveConfig) Validate() error {
	if c == nil || !model.ValidID(c.Tenant) || !model.ValidID(c.Stream) || !model.ValidID(c.KeyID) || !filepath.IsAbs(c.Directory) || len(c.TrustedKeys) == 0 || len(c.TrustedKeys) > 32 {
		return model.ErrInvalid
	}
	for id, value := range c.TrustedKeys {
		key, err := hex.DecodeString(value)
		if !model.ValidID(id) || err != nil || len(key) != 32 {
			return model.ErrInvalid
		}
	}
	if _, ok := c.TrustedKeys[c.KeyID]; !ok {
		return model.ErrInvalid
	}
	return nil
}

type RefreshBinding struct {
	Scope     string `json:"scope"`
	ProfileID string `json:"profile_id"`
}
type RefreshConfig struct {
	RedactionPolicyVersions []string                  `json:"redaction_policy_versions,omitempty"`
	SweepIntervalMinutes    int                       `json:"sweep_interval_minutes,omitempty"`
	Tenant                  string                    `json:"tenant"`
	Stream                  string                    `json:"stream"`
	Purpose                 string                    `json:"purpose"`
	Bindings                []RefreshBinding          `json:"bindings"`
	Profiles                []model.ExtractionProfile `json:"profiles"`
	MaxConcurrentPerTenant  int                       `json:"max_concurrent_per_tenant"`
	DailyTokenBudget        int64                     `json:"daily_token_budget"`
	CostUnit                string                    `json:"cost_unit"`
	DailyCostBudgetUnits    int64                     `json:"daily_cost_budget_units"`
}

func (c *RefreshConfig) Validate() error {
	if c == nil || len(c.RedactionPolicyVersions) > 64 || !model.ValidID(c.Tenant) || !model.ValidID(c.Stream) || len(c.Purpose) == 0 || len(c.Purpose) > 256 || len(c.Bindings) < 1 || len(c.Bindings) > 64 || len(c.Profiles) < 1 || len(c.Profiles) > 32 || c.MaxConcurrentPerTenant < 1 || c.MaxConcurrentPerTenant > 32 || c.DailyTokenBudget < 1 {
		return model.ErrInvalid
	}
	profiles := map[string]bool{}
	for _, p := range c.Profiles {
		if !model.ValidID(p.ID) || profiles[p.ID] || p.ParserVersion != "memory-refresh-json-v1" {
			return model.ErrInvalid
		}
		profiles[p.ID] = true
	}
	scopes := map[string]bool{}
	for _, b := range c.Bindings {
		if !model.ValidID(b.Scope) || scopes[b.Scope] || !profiles[b.ProfileID] {
			return model.ErrInvalid
		}
		scopes[b.Scope] = true
	}
	return nil
}
func (c *RefreshConfig) options() model.ExtractionOptions {
	o := model.ExtractionOptions{ScheduleOnly: true, MaxConcurrentPerTenant: c.MaxConcurrentPerTenant, DailyTokenBudget: c.DailyTokenBudget, CostUnit: c.CostUnit, DailyCostBudgetUnits: c.DailyCostBudgetUnits}
	for _, p := range c.Profiles {
		o.Profiles = append(o.Profiles, model.ApprovedExtractor{Profile: p})
	}
	return o
}
func (c *RefreshConfig) bindings() []model.MemoryRefreshBinding {
	out := make([]model.MemoryRefreshBinding, 0, len(c.Bindings))
	for _, b := range c.Bindings {
		out = append(out, model.MemoryRefreshBinding{Scope: b.Scope, ProfileID: b.ProfileID})
	}
	return out
}

type Config struct {
	OrphanCleanup          *workers.OrphanCleanupConfig `json:"orphan_cleanup,omitempty"`
	Telemetry              *runtimeconfig.Telemetry     `json:"telemetry,omitempty"`
	OpenFGATraceContext    bool                         `json:"openfga_trace_context,omitempty"`
	Extraction             *ExtractionConfig            `json:"extraction"`
	Refresh                *RefreshConfig               `json:"refresh"`
	Archive                *ArchiveConfig               `json:"archive"`
	Audit                  AuditConfig                  `json:"audit"`
	Mode                   string                       `json:"mode"`
	Principal              string                       `json:"principal"`
	Projection             runtimeconfig.LexicalRoute   `json:"projection"`
	OpenFGAURL             string                       `json:"openfga_url"`
	OpenFGAStore           string                       `json:"openfga_store"`
	OpenFGAModel           string                       `json:"openfga_model"`
	MaxConnections         int32                        `json:"max_connections"`
	MaxAttempts            int                          `json:"max_attempts"`
	LeaseSeconds           int                          `json:"lease_seconds"`
	PollMilliseconds       int                          `json:"poll_milliseconds"`
	MinBackoffMilliseconds int                          `json:"min_backoff_milliseconds"`
	MaxBackoffSeconds      int                          `json:"max_backoff_seconds"`
	DeliveryTimeoutSeconds int                          `json:"delivery_timeout_seconds"`
}

func LoadConfig(path string) (Config, error) {
	var c Config
	if err := runtimeconfig.Read(path, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.Telemetry.Validate() != nil {
		return model.ErrInvalid
	}
	if !model.ValidID(c.Principal) || c.OpenFGAURL == "" || !model.ValidID(c.OpenFGAStore) || !model.ValidID(c.OpenFGAModel) || c.MaxConnections < 1 || c.MaxConnections > 32 || c.DeliveryTimeoutSeconds < 1 || c.DeliveryTimeoutSeconds > 300 {
		return model.ErrInvalid
	}
	if c.Mode == "refresh-sweep" || c.Mode == "orphan-cleanup" {
		if c.MaxAttempts != 0 || c.LeaseSeconds != 0 || c.PollMilliseconds != 0 || c.MinBackoffMilliseconds != 0 || c.MaxBackoffSeconds != 0 {
			return model.ErrInvalid
		}
	} else if c.MaxAttempts < 1 || c.MaxAttempts > 100 || c.LeaseSeconds < 3 || c.LeaseSeconds > 300 || c.PollMilliseconds < 1 || c.PollMilliseconds > 60000 || c.MinBackoffMilliseconds < 1 || c.MaxBackoffSeconds < 1 || c.MaxBackoffSeconds > 86400 || c.MinBackoffMilliseconds > c.MaxBackoffSeconds*1000 {
		return model.ErrInvalid
	}
	if c.Mode != "orphan-cleanup" && c.OrphanCleanup != nil {
		return model.ErrInvalid
	}
	if c.Mode != "extraction" && c.Extraction != nil {
		return model.ErrInvalid
	}
	switch c.Mode {
	case "orphan-cleanup":
		if c.Refresh != nil || c.Archive != nil || c.Audit != (AuditConfig{}) || c.Projection != (runtimeconfig.LexicalRoute{}) || c.OrphanCleanup.Validate() != nil {
			return model.ErrInvalid
		}

	case "projection":
		if c.Refresh != nil || c.Archive != nil || c.Audit != (AuditConfig{}) || runtimeconfig.ValidateRoutes([]runtimeconfig.LexicalRoute{c.Projection}) != nil {
			return model.ErrInvalid
		}
	case "audit":
		if c.Refresh != nil || c.Archive != nil || c.Projection != (runtimeconfig.LexicalRoute{}) || !model.ValidID(c.Audit.Tenant) || !model.ValidID(c.Audit.Stream) || !model.ValidID(c.Audit.KeyID) {
			return model.ErrInvalid
		}
	case "archive":
		if c.Refresh != nil || c.Audit != (AuditConfig{}) || c.Projection != (runtimeconfig.LexicalRoute{}) || c.Archive.Validate() != nil {
			return model.ErrInvalid
		}
	case "refresh", "refresh-sweep":
		if c.Archive != nil || c.Audit != (AuditConfig{}) || c.Projection != (runtimeconfig.LexicalRoute{}) || c.Refresh.Validate() != nil {
			return model.ErrInvalid
		}
		if (c.Mode == "refresh" && c.Refresh.SweepIntervalMinutes != 0) || (c.Mode == "refresh-sweep" && (c.Refresh.SweepIntervalMinutes < 1 || c.Refresh.SweepIntervalMinutes > 1440)) {
			return model.ErrInvalid
		}
	case "extraction":
		if c.Refresh != nil || c.Archive != nil || c.Audit != (AuditConfig{}) || c.Projection != (runtimeconfig.LexicalRoute{}) || c.Extraction.Validate() != nil {
			return model.ErrInvalid
		}
	default:
		return model.ErrInvalid
	}
	return nil
}
