package daemon

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/mtlsidentity"
	"github.com/caxqueiroz/facts_engine/adapters/openfga"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"net"
	"os"
	"time"
)

func Run(ctx context.Context, c Config, logger *slog.Logger) error {
	return run(ctx, c, logger, nil)
}

// RunOnListener uses a host-supplied listener and closes it on return, including
// startup failure. Dependencies remain owned by the service composition.
func RunOnListener(ctx context.Context, c Config, logger *slog.Logger, listener net.Listener) error {
	if listener == nil {
		return errors.New("missing listener")
	}
	defer listener.Close()
	return run(ctx, c, logger, listener)
}
func run(ctx context.Context, c Config, logger *slog.Logger, listener net.Listener) error {
	if err := c.Validate(); err != nil {
		return errors.New("invalid service configuration")
	}
	encryptionKey, err := runtimeconfig.HexKey("MEMORY_ENCRYPTION_KEY_HEX", 32)
	if err != nil {
		return err
	}
	paginationKey, err := runtimeconfig.HexKey("MEMORY_PAGINATION_KEY_HEX", 32)
	if err != nil {
		return err
	}
	cert, err := tls.LoadX509KeyPair(c.TLSCertificate, c.TLSKey)
	if err != nil {
		return errors.New("cannot load server TLS key pair")
	}
	ca, err := os.ReadFile(c.ClientCA)
	if err != nil {
		return errors.New("cannot load client CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid client CA")
	}
	authn, err := mtlsidentity.New(c.Identities)
	if err != nil {
		return errors.New("invalid workload bindings")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, NextProtos: []string{"h2", "http/1.1"}}
	dsn := os.Getenv("MEMORY_DATABASE_URL")
	if dsn == "" {
		return errors.New("missing database configuration")
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	poolConfig.MaxConns = c.MaxConnections
	poolConfig.ConnConfig.ConnectTimeout = 5 * time.Second
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(startup, poolConfig)
	if err != nil {
		return errors.New("cannot create database pool")
	}
	defer pool.Close()
	cipher, err := postgres.NewAESGCM(encryptionKey)
	if err != nil {
		return errors.New("invalid encryption key")
	}
	ledger, err := postgres.New(startup, pool, cipher)
	if err != nil {
		return errors.New("ledger schema or runtime role check failed")
	}
	store, err := evidence.NewPostgres(pool, cipher)
	if err != nil {
		return errors.New("cannot initialize evidence store")
	}
	authz, err := openfga.New(openfga.Config{PropagateTraceContext: c.OpenFGATraceContext, URL: c.OpenFGAURL, StoreID: c.OpenFGAStore, ModelID: c.OpenFGAModel, Token: os.Getenv("MEMORY_OPENFGA_TOKEN")})
	if err != nil {
		return errors.New("invalid authorization configuration")
	}
	options := model.EngineOptions{Reads: model.ReadOptions{PaginationKey: paginationKey}}
	if c.Extraction != nil {
		options.Extraction = c.Extraction.Options()
		options.RedactionPolicyVersions = append([]string(nil), c.Extraction.RedactionPolicyVersions...)
	}
	if c.AuditKeyID != "" {
		key, err := runtimeconfig.HexKey("MEMORY_AUDIT_PRIVATE_KEY_HEX", ed25519.PrivateKeySize)
		if err != nil {
			return err
		}
		signer, err := audit.NewEd25519(c.AuditKeyID, key)
		if err != nil {
			return errors.New("invalid audit signing key")
		}
		options.Audit.Signer = signer
	}
	if len(c.Retrieval) > 0 {
		routes, closeRoutes, err := runtimeconfig.OpenLexical(startup, c.Retrieval)
		if err != nil {
			return err
		}
		defer closeRoutes()
		options.Retrieval = model.RetrievalOptions{Index: routes, Registry: ledger, Progress: ledger}
		if c.OperationsScope != "" {
			dsn := os.Getenv("MEMORY_OPERATIONS_DATABASE_URL")
			if dsn == "" {
				return errors.New("missing administrative database configuration")
			}
			config, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				return errors.New("invalid administrative database configuration")
			}
			config.MaxConns = min(c.MaxConnections, 4)
			config.ConnConfig.ConnectTimeout = 5 * time.Second
			adminPool, err := pgxpool.NewWithConfig(startup, config)
			if err != nil {
				return errors.New("cannot create administrative database pool")
			}
			defer adminPool.Close()
			registry, err := postgres.New(startup, adminPool, cipher)
			if err != nil {
				return errors.New("administrative schema or role check failed")
			}
			options.Operations = model.OperationsOptions{Scope: c.OperationsScope, Store: registry, Index: routes, Progress: registry}
		}
	}
	engine, err := facts.NewWithOptions(ledger, authz, store, options)
	if err != nil {
		return errors.New("cannot initialize engine")
	}
	timeout := time.Duration(c.RPCTimeoutSeconds) * time.Second
	handler, err := connectapi.NewHandlerWithOptions(engine, authn.Verify, connectapi.Limits{Timeout: timeout, MaxConcurrent: c.MaxConcurrent, MaxRequestBytes: c.MaxRequestBytes, MaxResponseBytes: c.MaxResponseBytes}, connectapi.HandlerOptions{AcceptTraceContext: c.AcceptTraceContext})
	if err != nil {
		return errors.New("cannot initialize API")
	}
	first := c.Identities[0]
	probe := func(ctx context.Context) map[string]bool {
		databaseOK := pool.Ping(ctx) == nil
		_, err := authz.Check(ctx, model.AuthorizationRequest{Caller: model.Caller{Tenant: first.Tenant, Principal: first.Principal, Service: true}, Scope: c.ReadinessScope, Permission: "can_read", Purpose: "readiness"})
		// A valid denial proves the pinned model is reachable; readiness grants no permission.
		return map[string]bool{"ledger": databaseOK, "authorization": err == nil}
	}
	if listener == nil {
		listener, err = net.Listen("tcp", c.Listen)
		if err != nil {
			return errors.New("cannot listen on configured address")
		}
	}
	defer listener.Close()
	return Serve(ctx, listener, tlsConfig, handler, authn.Wrap, probe, timeout, time.Duration(c.ShutdownSeconds)*time.Second, logger)
}
