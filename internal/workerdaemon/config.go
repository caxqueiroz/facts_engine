package workerdaemon

import (
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/model"
)

type AuditConfig struct {
	Tenant string `json:"tenant"`
	Stream string `json:"stream"`
	KeyID  string `json:"key_id"`
}
type Config struct {
	Audit                  AuditConfig                `json:"audit"`
	Mode                   string                     `json:"mode"`
	Principal              string                     `json:"principal"`
	Projection             runtimeconfig.LexicalRoute `json:"projection"`
	OpenFGAURL             string                     `json:"openfga_url"`
	OpenFGAStore           string                     `json:"openfga_store"`
	OpenFGAModel           string                     `json:"openfga_model"`
	MaxConnections         int32                      `json:"max_connections"`
	MaxAttempts            int                        `json:"max_attempts"`
	LeaseSeconds           int                        `json:"lease_seconds"`
	PollMilliseconds       int                        `json:"poll_milliseconds"`
	MinBackoffMilliseconds int                        `json:"min_backoff_milliseconds"`
	MaxBackoffSeconds      int                        `json:"max_backoff_seconds"`
	DeliveryTimeoutSeconds int                        `json:"delivery_timeout_seconds"`
}

func LoadConfig(path string) (Config, error) {
	var c Config
	if err := runtimeconfig.Read(path, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if !model.ValidID(c.Principal) || c.OpenFGAURL == "" || !model.ValidID(c.OpenFGAStore) || !model.ValidID(c.OpenFGAModel) || c.MaxConnections < 1 || c.MaxConnections > 32 || c.MaxAttempts < 1 || c.MaxAttempts > 100 || c.LeaseSeconds < 3 || c.LeaseSeconds > 300 || c.PollMilliseconds < 1 || c.PollMilliseconds > 60000 || c.MinBackoffMilliseconds < 1 || c.MaxBackoffSeconds < 1 || c.MaxBackoffSeconds > 86400 || c.MinBackoffMilliseconds > c.MaxBackoffSeconds*1000 || c.DeliveryTimeoutSeconds < 1 || c.DeliveryTimeoutSeconds > 300 {
		return model.ErrInvalid
	}
	switch c.Mode {
	case "projection":
		if c.Audit != (AuditConfig{}) || runtimeconfig.ValidateRoutes([]runtimeconfig.LexicalRoute{c.Projection}) != nil {
			return model.ErrInvalid
		}
	case "audit":
		if c.Projection != (runtimeconfig.LexicalRoute{}) || !model.ValidID(c.Audit.Tenant) || !model.ValidID(c.Audit.Stream) || !model.ValidID(c.Audit.KeyID) {
			return model.ErrInvalid
		}
	default:
		return model.ErrInvalid
	}
	return nil
}
