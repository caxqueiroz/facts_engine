package workerdaemon

import (
	"encoding/hex"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/model"
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

type Config struct {
	Archive                *ArchiveConfig             `json:"archive"`
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
		if c.Archive != nil || c.Audit != (AuditConfig{}) || runtimeconfig.ValidateRoutes([]runtimeconfig.LexicalRoute{c.Projection}) != nil {
			return model.ErrInvalid
		}
	case "audit":
		if c.Archive != nil || c.Projection != (runtimeconfig.LexicalRoute{}) || !model.ValidID(c.Audit.Tenant) || !model.ValidID(c.Audit.Stream) || !model.ValidID(c.Audit.KeyID) {
			return model.ErrInvalid
		}
	case "archive":
		if c.Audit != (AuditConfig{}) || c.Projection != (runtimeconfig.LexicalRoute{}) || c.Archive.Validate() != nil {
			return model.ErrInvalid
		}
	default:
		return model.ErrInvalid
	}
	return nil
}
