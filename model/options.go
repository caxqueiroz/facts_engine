package model

// EngineOptions composes optional capabilities without global configuration.
// Hosts own every injected connection, provider, signing key and worker lifecycle.
type EngineOptions struct {
	Telemetry               TelemetryOptions
	Operations              OperationsOptions
	RedactionPolicyVersions []string
	AttestationIdentity     AttestationIdentityOptions
	Extraction              ExtractionOptions
	Audit                   AuditOptions
	Retrieval               RetrievalOptions
	Reads                   ReadOptions
}
