package daemon

import (
	"github.com/caxqueiroz/facts_engine/adapters/mtlsidentity"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/model"
)

// Config contains deployment settings, never credentials. Secrets are supplied
// through environment variables by the host's secret manager.
type ContextArchivePolicy struct {
	Stream              string `json:"stream"`
	MaxPendingRevisions int64  `json:"max_pending_revisions"`
}

type Config struct {
	AttestationIdentity *runtimeconfig.AttestationIdentity  `json:"attestation_identity,omitempty"`
	Gateways            []mtlsidentity.GatewayBinding       `json:"gateways,omitempty"`
	Telemetry           *runtimeconfig.Telemetry            `json:"telemetry,omitempty"`
	ContextArchive      *ContextArchivePolicy               `json:"context_archive,omitempty"`
	AcceptTraceContext  bool                                `json:"accept_trace_context,omitempty"`
	OpenFGATraceContext bool                                `json:"openfga_trace_context,omitempty"`
	Extraction          *runtimeconfig.ExtractionScheduling `json:"extraction,omitempty"`
	Retrieval           []runtimeconfig.LexicalRoute        `json:"retrieval"`
	OperationsScope     string                              `json:"operations_scope"`
	Listen              string                              `json:"listen"`
	TLSCertificate      string                              `json:"tls_certificate"`
	TLSKey              string                              `json:"tls_key"`
	ClientCA            string                              `json:"client_ca"`
	Identities          []mtlsidentity.Binding              `json:"identities"`
	OpenFGAURL          string                              `json:"openfga_url"`
	OpenFGAStore        string                              `json:"openfga_store"`
	OpenFGAModel        string                              `json:"openfga_model"`
	ReadinessScope      string                              `json:"readiness_scope"`
	AuditKeyID          string                              `json:"audit_key_id"`
	MaxConnections      int32                               `json:"max_connections"`
	MaxConcurrent       int                                 `json:"max_concurrent"`
	MaxRequestBytes     int                                 `json:"max_request_bytes"`
	MaxResponseBytes    int                                 `json:"max_response_bytes"`
	RPCTimeoutSeconds   int                                 `json:"rpc_timeout_seconds"`
	ShutdownSeconds     int                                 `json:"shutdown_seconds"`
}

func LoadConfig(path string) (Config, error) {
	var c Config
	if err := runtimeconfig.Read(path, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.AttestationIdentity.Validate() != nil {
		return model.ErrInvalid
	}
	if c.Telemetry.Validate() != nil {
		return model.ErrInvalid
	}
	if c.ContextArchive != nil && (!model.ValidID(c.ContextArchive.Stream) || c.ContextArchive.MaxPendingRevisions < 0) {
		return model.ErrInvalid
	}
	if c.Listen == "" || c.TLSCertificate == "" || c.TLSKey == "" || c.ClientCA == "" || c.OpenFGAURL == "" || !model.ValidID(c.OpenFGAStore) || !model.ValidID(c.OpenFGAModel) || !model.ValidID(c.ReadinessScope) || (c.AuditKeyID != "" && !model.ValidID(c.AuditKeyID)) || c.MaxConnections < 1 || c.MaxConnections > 256 || c.MaxConcurrent < 1 || c.MaxConcurrent > 1024 || c.MaxRequestBytes < 1 || c.MaxRequestBytes > 32<<20 || c.MaxResponseBytes < 1 || c.MaxResponseBytes > 256<<20 || c.RPCTimeoutSeconds < 1 || c.RPCTimeoutSeconds > 120 || c.ShutdownSeconds < 1 || c.ShutdownSeconds > 300 {
		return model.ErrInvalid
	}
	if c.Extraction != nil && c.Extraction.Validate() != nil {
		return model.ErrInvalid
	}
	if err := runtimeconfig.ValidateRoutes(c.Retrieval); err != nil {
		return err
	}
	if c.OperationsScope != "" && (!model.ValidID(c.OperationsScope) || len(c.Retrieval) == 0) {
		return model.ErrInvalid
	}
	_, err := mtlsidentity.NewWithGateways(c.Identities, c.Gateways)
	return err
}
