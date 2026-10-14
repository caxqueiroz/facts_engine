package daemon

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/caxqueiroz/facts_engine/adapters/mtlsidentity"
	"github.com/caxqueiroz/facts_engine/model"
	"io"
	"os"
)

// Config contains deployment settings, never credentials. Secrets are supplied
// through environment variables by the host's secret manager.
type Config struct {
	Listen            string                 `json:"listen"`
	TLSCertificate    string                 `json:"tls_certificate"`
	TLSKey            string                 `json:"tls_key"`
	ClientCA          string                 `json:"client_ca"`
	Identities        []mtlsidentity.Binding `json:"identities"`
	OpenFGAURL        string                 `json:"openfga_url"`
	OpenFGAStore      string                 `json:"openfga_store"`
	OpenFGAModel      string                 `json:"openfga_model"`
	ReadinessScope    string                 `json:"readiness_scope"`
	AuditKeyID        string                 `json:"audit_key_id"`
	MaxConnections    int32                  `json:"max_connections"`
	MaxConcurrent     int                    `json:"max_concurrent"`
	MaxRequestBytes   int                    `json:"max_request_bytes"`
	MaxResponseBytes  int                    `json:"max_response_bytes"`
	RPCTimeoutSeconds int                    `json:"rpc_timeout_seconds"`
	ShutdownSeconds   int                    `json:"shutdown_seconds"`
}

func LoadConfig(path string) (Config, error) {
	var c Config
	file, err := os.Open(path)
	if err != nil {
		return c, errors.New("cannot open configuration")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return Config{}, errors.New("configuration exceeds size limit or cannot be read")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return Config{}, errors.New("invalid configuration JSON")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return Config{}, errors.New("unexpected configuration data")
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.Listen == "" || c.TLSCertificate == "" || c.TLSKey == "" || c.ClientCA == "" || c.OpenFGAURL == "" || !model.ValidID(c.OpenFGAStore) || !model.ValidID(c.OpenFGAModel) || !model.ValidID(c.ReadinessScope) || (c.AuditKeyID != "" && !model.ValidID(c.AuditKeyID)) || c.MaxConnections < 1 || c.MaxConnections > 256 || c.MaxConcurrent < 1 || c.MaxConcurrent > 1024 || c.MaxRequestBytes < 1 || c.MaxRequestBytes > 32<<20 || c.MaxResponseBytes < 1 || c.MaxResponseBytes > 256<<20 || c.RPCTimeoutSeconds < 1 || c.RPCTimeoutSeconds > 120 || c.ShutdownSeconds < 1 || c.ShutdownSeconds > 300 {
		return model.ErrInvalid
	}
	_, err := mtlsidentity.New(c.Identities)
	return err
}
