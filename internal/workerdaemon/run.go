package workerdaemon

import (
	"context"
	"errors"
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

// Run owns its pools and runs one explicitly configured tenant/generation. It
// cancels in-flight delivery on shutdown; unacknowledged leases remain retryable.
func Run(ctx context.Context, c Config, logger *slog.Logger) error {
	if c.Validate() != nil || logger == nil {
		return errors.New("invalid worker configuration")
	}
	key, err := runtimeconfig.HexKey("MEMORY_ENCRYPTION_KEY_HEX", 32)
	if err != nil {
		return err
	}
	dsn := os.Getenv("MEMORY_WORKER_DATABASE_URL")
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
	index, closeIndex, err := runtimeconfig.OpenLexical(startup, []runtimeconfig.LexicalRoute{c.Projection})
	if err != nil {
		return err
	}
	defer closeIndex()
	auth, err := openfga.New(openfga.Config{URL: c.OpenFGAURL, StoreID: c.OpenFGAStore, ModelID: c.OpenFGAModel, Token: os.Getenv("MEMORY_OPENFGA_TOKEN")})
	if err != nil {
		return errors.New("invalid authorization configuration")
	}
	caller := model.Caller{Tenant: c.Projection.Tenant, Principal: c.Principal, Service: true}
	handler, err := workers.NewProjection(ledger, index, auth, caller, c.Projection.Generation)
	if err != nil {
		return errors.New("cannot initialize projection handler")
	}
	consumer := model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.ProjectionConsumer(c.Projection.Generation)}
	if err := ledger.RegisterConsumer(startup, consumer, c.MaxAttempts); err != nil {
		return errors.New("cannot register configured consumer")
	}
	worker, err := workers.NewOutbox(ledger, consumer, handler, workers.Options{Lease: time.Duration(c.LeaseSeconds) * time.Second, PollInterval: time.Duration(c.PollMilliseconds) * time.Millisecond, MinBackoff: time.Duration(c.MinBackoffMilliseconds) * time.Millisecond, MaxBackoff: time.Duration(c.MaxBackoffSeconds) * time.Second})
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
