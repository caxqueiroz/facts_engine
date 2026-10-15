package workerdaemon

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/archive"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/openfga"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/internal/runtimeconfig"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/caxqueiroz/facts_engine/workers"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"os"
	"time"
)

// Run owns its pools and runs one explicitly configured tenant and worker mode. It
// cancels in-flight delivery on shutdown; unacknowledged leases remain retryable.
func Run(ctx context.Context, c Config, logger *slog.Logger) error {
	if c.Validate() != nil || logger == nil {
		return errors.New("invalid worker configuration")
	}
	telemetry, err := runtimeconfig.StartTelemetry(ctx, c.Telemetry, "memoryworker", logger)
	if err != nil {
		return err
	}
	defer telemetry.Close(logger)
	key, err := runtimeconfig.HexKey("MEMORY_ENCRYPTION_KEY_HEX", 32)
	if err != nil {
		return err
	}
	dsn := os.Getenv("MEMORY_WORKER_DATABASE_URL")
	if c.Mode == "refresh-sweep" {
		dsn = os.Getenv("MEMORY_DATABASE_URL")
	}
	if dsn == "" {
		return errors.New("missing worker database configuration")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("invalid worker database configuration")
	}
	config.MaxConns = c.MaxConnections
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(startup, config)
	if err != nil {
		return errors.New("cannot create worker pool")
	}
	defer pool.Close()
	cipher, err := postgres.NewAESGCM(key)
	if err != nil {
		return errors.New("invalid encryption key")
	}
	ledger, err := postgres.New(startup, pool, cipher)
	if err != nil {
		return errors.New("worker schema or role check failed")
	}
	auth, err := openfga.New(openfga.Config{PropagateTraceContext: c.OpenFGATraceContext, URL: c.OpenFGAURL, StoreID: c.OpenFGAStore, ModelID: c.OpenFGAModel, Token: os.Getenv("MEMORY_OPENFGA_TOKEN")})
	if err != nil {
		return errors.New("invalid authorization configuration")
	}
	var handler workers.Handler
	var consumer model.ConsumerKey
	switch c.Mode {
	case "orphan-cleanup":
		store, err := evidence.NewPostgres(pool, cipher)
		if err != nil {
			return errors.New("cannot initialize cleanup evidence store")
		}
		engine, err := facts.NewWithOptions(ledger, auth, store, model.EngineOptions{Telemetry: telemetry.Options})
		if err != nil {
			return errors.New("cannot initialize cleanup engine")
		}
		caller := model.Caller{Tenant: c.OrphanCleanup.Tenant, Principal: c.Principal, Service: true}
		ticker, err := workers.NewOrphanCleanup(engine, caller, *c.OrphanCleanup)
		if err != nil {
			return errors.New("cannot initialize cleanup worker")
		}
		logger.Info("worker started", "mode", c.Mode)
		err = RunOrphanLoop(ctx, ticker, time.Duration(c.OrphanCleanup.IntervalMinutes)*time.Minute, time.Duration(c.DeliveryTimeoutSeconds)*time.Second, logger)
		logger.Info("worker stopped", "mode", c.Mode, "failed", err != nil)
		if err != nil {
			return errors.New("worker stopped after orphan cleanup failure")
		}
		return nil

	case "projection":
		index, closeIndex, err := runtimeconfig.OpenLexical(startup, []runtimeconfig.LexicalRoute{c.Projection})
		if err != nil {
			return err
		}
		defer closeIndex()
		caller := model.Caller{Tenant: c.Projection.Tenant, Principal: c.Principal, Service: true}
		handler, err = workers.NewProjectionWithTelemetry(ledger, index, auth, caller, c.Projection.Generation, telemetry.Options)
		if err != nil {
			return errors.New("cannot initialize projection handler")
		}
		consumer = model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.ProjectionConsumer(c.Projection.Generation)}
	case "archive":
		archiveKey, err := runtimeconfig.HexKey("MEMORY_ARCHIVE_ENCRYPTION_KEY_HEX", 32)
		if err != nil {
			return err
		}
		archiveCipher, err := postgres.NewAESGCM(archiveKey)
		if err != nil {
			return errors.New("invalid archive encryption key")
		}
		objects, err := archive.NewFiles(c.Archive.Directory, archiveCipher)
		if err != nil {
			return errors.New("cannot initialize configured archive directory")
		}
		private, err := runtimeconfig.HexKey("MEMORY_ARCHIVE_PRIVATE_KEY_HEX", ed25519.PrivateKeySize)
		if err != nil {
			return err
		}
		keys := make(map[string]ed25519.PublicKey, len(c.Archive.TrustedKeys))
		for id, value := range c.Archive.TrustedKeys {
			key, err := hex.DecodeString(value)
			if err != nil {
				return errors.New("invalid trusted archive public key")
			}
			keys[id] = key
		}
		if !bytes.Equal(ed25519.PrivateKey(private).Public().(ed25519.PublicKey), keys[c.Archive.KeyID]) {
			return errors.New("archive signing key does not match configured trust")
		}
		signer, err := audit.NewEd25519(c.Archive.KeyID, private)
		if err != nil {
			return errors.New("invalid archive signing key")
		}
		caller := model.Caller{Tenant: c.Archive.Tenant, Principal: c.Principal, Service: true}
		handler, err = workers.NewArchiveWithTelemetry(ledger, objects, signer, keys, auth, caller, c.Archive.Stream, telemetry.Options)
		if err != nil {
			return errors.New("cannot initialize archive handler")
		}
		consumer = model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.ArchiveConsumer(c.Archive.Stream)}
	case "refresh", "refresh-sweep":
		enginePool := pool
		engineLedger := ledger
		if c.Mode == "refresh" {
			dsn := os.Getenv("MEMORY_DATABASE_URL")
			if dsn == "" {
				return errors.New("missing worker engine database configuration")
			}
			engineConfig, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				return errors.New("invalid worker engine database configuration")
			}
			engineConfig.MaxConns = c.MaxConnections
			engineConfig.ConnConfig.ConnectTimeout = 5 * time.Second
			enginePool, err = pgxpool.NewWithConfig(startup, engineConfig)
			if err != nil {
				return errors.New("cannot create worker engine pool")
			}
			defer enginePool.Close()
			engineLedger, err = postgres.New(startup, enginePool, cipher)
			if err != nil {
				return errors.New("worker engine schema or role check failed")
			}
		}
		store, err := evidence.NewPostgres(enginePool, cipher)
		if err != nil {
			return errors.New("cannot initialize worker evidence store")
		}

		engine, err := facts.NewWithOptions(engineLedger, auth, store, model.EngineOptions{Telemetry: telemetry.Options, Extraction: c.Refresh.options(), RedactionPolicyVersions: c.Refresh.RedactionPolicyVersions})
		if err != nil {
			return errors.New("invalid refresh engine or approved profile configuration")
		}
		caller := model.Caller{Tenant: c.Refresh.Tenant, Principal: c.Principal, Service: true}
		if c.Mode == "refresh-sweep" {
			interval := time.Duration(c.Refresh.SweepIntervalMinutes) * time.Minute
			ticker, err := workers.NewMemoryRefreshTicker(engine, caller, c.Refresh.Stream, c.Refresh.Purpose, c.Refresh.bindings(), interval)
			if err != nil {
				return errors.New("cannot initialize refresh sweep ticker")
			}
			logger.Info("worker started", "mode", c.Mode)
			err = RunSweepLoop(ctx, ticker, interval, time.Duration(c.DeliveryTimeoutSeconds)*time.Second)
			logger.Info("worker stopped", "mode", c.Mode, "failed", err != nil)
			if err != nil {
				return errors.New("worker stopped after sweep failure")
			}
			return nil
		}
		handler, err = workers.NewMemoryRefresh(engine, caller, c.Refresh.Stream, c.Refresh.Purpose, c.Refresh.bindings())
		if err != nil {
			return errors.New("cannot initialize refresh scheduler")
		}
		consumer = model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.MemoryRefreshConsumer(c.Refresh.Stream)}
	case "extraction":
		dsn := os.Getenv("MEMORY_DATABASE_URL")
		if dsn == "" {
			return errors.New("missing extraction engine database configuration")
		}
		engineConfig, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			return errors.New("invalid extraction engine database configuration")
		}
		engineConfig.MaxConns = c.MaxConnections
		engineConfig.ConnConfig.ConnectTimeout = 5 * time.Second
		enginePool, err := pgxpool.NewWithConfig(startup, engineConfig)
		if err != nil {
			return errors.New("cannot create extraction engine pool")
		}
		defer enginePool.Close()
		engineLedger, err := postgres.New(startup, enginePool, cipher)
		if err != nil {
			return errors.New("extraction engine schema or role check failed")
		}
		store, err := evidence.NewPostgres(enginePool, cipher)
		if err != nil {
			return errors.New("cannot initialize extraction evidence store")
		}

		extraction, closeProviders, err := openExtractionGateways(c.Extraction)
		if err != nil {
			return err
		}
		defer closeProviders()
		engine, err := facts.NewWithOptions(engineLedger, auth, store, model.EngineOptions{Telemetry: telemetry.Options, Extraction: extraction, RedactionPolicyVersions: c.Extraction.Scheduling.RedactionPolicyVersions})
		if err != nil {
			return errors.New("invalid extraction engine or approved profiles")
		}
		caller := model.Caller{Tenant: c.Extraction.Tenant, Principal: c.Principal, Service: true}
		handler, err = workers.NewExtraction(engine, caller, c.Extraction.Stream)
		if err != nil {
			return errors.New("cannot initialize extraction executor")
		}
		consumer = model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.ExtractionConsumer(c.Extraction.Stream)}
	case "audit":
		signingKey, err := runtimeconfig.HexKey("MEMORY_AUDIT_PRIVATE_KEY_HEX", ed25519.PrivateKeySize)
		if err != nil {
			return err
		}
		signer, err := audit.NewEd25519(c.Audit.KeyID, signingKey)
		if err != nil {
			return errors.New("invalid audit signing key")
		}
		dsn := os.Getenv("MEMORY_DATABASE_URL")
		if dsn == "" {
			return errors.New("missing audit engine database configuration")
		}
		engineConfig, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			return errors.New("invalid audit engine database configuration")
		}
		engineConfig.MaxConns = c.MaxConnections
		engineConfig.ConnConfig.ConnectTimeout = 5 * time.Second
		enginePool, err := pgxpool.NewWithConfig(startup, engineConfig)
		if err != nil {
			return errors.New("cannot create audit engine pool")
		}
		defer enginePool.Close()
		engineLedger, err := postgres.New(startup, enginePool, cipher)
		if err != nil {
			return errors.New("audit engine schema or role check failed")
		}
		store, err := evidence.NewPostgres(enginePool, cipher)
		if err != nil {
			return errors.New("cannot initialize audit evidence store")
		}
		engine, err := facts.NewWithOptions(engineLedger, auth, store, model.EngineOptions{Telemetry: telemetry.Options, Audit: model.AuditOptions{Signer: signer}})
		if err != nil {
			return errors.New("cannot initialize audit engine")
		}
		caller := model.Caller{Tenant: c.Audit.Tenant, Principal: c.Principal, Service: true}
		handler, err = workers.NewAuditExport(engine, auth, caller, c.Audit.Stream)
		if err != nil {
			return errors.New("cannot initialize audit handler")
		}
		consumer = model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.AuditExportConsumer(c.Audit.Stream)}
	}
	if err := ledger.RegisterConsumer(startup, consumer, c.MaxAttempts); err != nil {
		return errors.New("cannot register configured consumer")
	}
	if telemetry.Metrics != nil {
		monitor, err := workers.NewProgressMonitor(ledger, []workers.ProgressTarget{{Key: consumer, Kind: c.Mode}}, telemetry.Metrics, 2*time.Second)
		if err != nil {
			return errors.New("cannot initialize progress telemetry")
		}
		defer monitor.Close()
	}
	worker, err := workers.NewOutbox(ledger, consumer, handler, workers.Options{Telemetry: telemetry.Options, Lease: time.Duration(c.LeaseSeconds) * time.Second, PollInterval: time.Duration(c.PollMilliseconds) * time.Millisecond, MinBackoff: time.Duration(c.MinBackoffMilliseconds) * time.Millisecond, MaxBackoff: time.Duration(c.MaxBackoffSeconds) * time.Second})
	if err != nil {
		return errors.New("cannot initialize outbox worker")
	}
	logger.Info("worker started", "mode", c.Mode)
	err = RunLoop(ctx, checkedProcessor{Processor: worker, queue: ledger, key: consumer}, time.Duration(c.DeliveryTimeoutSeconds)*time.Second, time.Duration(c.PollMilliseconds)*time.Millisecond)
	logger.Info("worker stopped", "mode", c.Mode, "failed", err != nil)
	if err != nil {
		return errors.New("worker stopped after delivery or queue failure")
	}
	return nil
}

type Processor interface {
	ProcessOne(context.Context) (bool, error)
}

func RunLoop(ctx context.Context, processor Processor, timeout, poll time.Duration) error {
	if processor == nil || timeout <= 0 || poll <= 0 {
		return model.ErrInvalid
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		delivery, cancel := context.WithTimeout(ctx, timeout)
		worked, err := processor.ProcessOne(delivery)
		if err == nil {
			err = delivery.Err()
		}
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		// A timeout is an operational failure, not an acknowledgement or an automatic
		// retry reset. The external supervisor decides whether to restart the process.
		if err != nil {
			return err
		}
		if worked {
			continue
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

type checkedProcessor struct {
	Processor
	queue model.Outbox
	key   model.ConsumerKey
}

func (p checkedProcessor) ProcessOne(ctx context.Context) (bool, error) {
	worked, err := p.Processor.ProcessOne(ctx)
	if err != nil || worked {
		return worked, err
	}
	status, err := p.queue.ConsumerStatus(ctx, p.key)
	if err != nil {
		return false, err
	}
	if status.State == "failed" {
		return false, model.ErrPrecondition
	}
	return false, nil
}
