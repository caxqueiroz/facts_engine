package daemon

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/connectapi"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/mtlsidentity"
	"github.com/caxqueiroz/facts_engine/adapters/openfga"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"net"
	"os"
	"time"
)

func secretHex(name string, size int) ([]byte, error) {
	value, err := hex.DecodeString(os.Getenv(name))
	if err != nil || len(value) != size {
		return nil, errors.New("missing or invalid key configuration")
	}
	return value, nil
}
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
	encryptionKey, err := secretHex("MEMORY_ENCRYPTION_KEY_HEX", 32)
	if err != nil {
		return err
	}
	paginationKey, err := secretHex("MEMORY_PAGINATION_KEY_HEX", 32)
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
	authz, err := openfga.New(openfga.Config{URL: c.OpenFGAURL, StoreID: c.OpenFGAStore, ModelID: c.OpenFGAModel, Token: os.Getenv("MEMORY_OPENFGA_TOKEN")})
	if err != nil {
		return errors.New("invalid authorization configuration")
	}
	options := model.EngineOptions{Reads: model.ReadOptions{PaginationKey: paginationKey}}
	if c.AuditKeyID != "" {
		key, err := secretHex("MEMORY_AUDIT_PRIVATE_KEY_HEX", ed25519.PrivateKeySize)
		if err != nil {
			return err
		}
		signer, err := audit.NewEd25519(c.AuditKeyID, key)
		if err != nil {
			return errors.New("invalid audit signing key")
		}
		options.Audit.Signer = signer
	}
	engine, err := facts.NewWithOptions(ledger, authz, store, options)
	if err != nil {
		return errors.New("cannot initialize engine")
	}
	timeout := time.Duration(c.RPCTimeoutSeconds) * time.Second
	handler, err := connectapi.NewHandler(engine, authn.Verify, connectapi.Limits{Timeout: timeout, MaxConcurrent: c.MaxConcurrent, MaxRequestBytes: c.MaxRequestBytes, MaxResponseBytes: c.MaxResponseBytes})
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
