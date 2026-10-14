// Package workers provides explicit, cancellable worker execution. Constructors
// never start goroutines. Hosts own pools, tenant configuration, and shutdown.
package workers

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"math/rand/v2"
	"time"
)

type Handler interface {
	Handle(context.Context, model.Delivery) error
}
type HandlerFunc func(context.Context, model.Delivery) error

func (f HandlerFunc) Handle(ctx context.Context, d model.Delivery) error { return f(ctx, d) }

type Options struct{ Lease, PollInterval, MinBackoff, MaxBackoff time.Duration }
type Worker struct {
	queue   model.Outbox
	key     model.ConsumerKey
	handler Handler
	options Options
}

func NewOutbox(queue model.Outbox, key model.ConsumerKey, handler Handler, o Options) (*Worker, error) {
	if queue == nil || handler == nil || !model.ValidID(key.Tenant) || !model.ValidID(key.Consumer) || o.Lease < 3*time.Second || o.Lease > 5*time.Minute || o.PollInterval < time.Millisecond || o.PollInterval > time.Minute || o.MinBackoff < time.Millisecond || o.MaxBackoff < o.MinBackoff || o.MaxBackoff > 24*time.Hour {
		return nil, model.ErrInvalid
	}
	return &Worker{queue, key, handler, o}, nil
}

// ProcessOne handles at most one event. False means no event was claimable.
// A handler failure is durably scheduled for retry; database/lease errors return
// to the host. Handlers must honor cancellation and deduplicate external effects.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	d, err := w.queue.ClaimDelivery(ctx, w.key, w.options.Lease)
	if errors.Is(err, model.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	stopHeartbeat := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(w.options.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				done <- nil
				return
			case <-work.Done():
				done <- nil
				return
			case <-ticker.C:
				renewal, stop := context.WithTimeout(work, w.options.Lease/3)
				err := w.queue.RenewDelivery(renewal, d, w.options.Lease)
				stop()
				if err != nil {
					cancel()
					done <- err
					return
				}
			}
		}
	}()
	handlerErr := w.handler.Handle(work, d)
	close(stopHeartbeat)
	renewErr := <-done
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if renewErr != nil {
		return true, renewErr
	}
	if handlerErr != nil {
		delay := w.backoff(d.Attempt)
		var retry *retryAfterError
		if errors.As(handlerErr, &retry) {
			delay = max(delay, retry.delay)
		}
		return true, w.queue.FailDelivery(ctx, d, delay)
	}
	return true, w.queue.AcknowledgeDelivery(ctx, d)
}
func (w *Worker) backoff(attempt int) time.Duration {
	cap := w.options.MinBackoff
	for i := 1; i < attempt && cap < w.options.MaxBackoff; i++ {
		if cap > w.options.MaxBackoff/2 {
			cap = w.options.MaxBackoff
			break
		}
		cap *= 2
	}
	if cap > w.options.MaxBackoff {
		cap = w.options.MaxBackoff
	}
	// Equal jitter keeps a positive delay and avoids synchronized retries.
	return max(time.Millisecond, cap/2+time.Duration(rand.Int64N(int64(cap-cap/2)+1)))
}

// Run polls until cancelled. Database errors return to the host supervisor;
// failed consumers remain visible and require an explicit operator retry.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		worked, err := w.ProcessOne(ctx)
		if err != nil {
			return err
		}
		if worked {
			continue
		}
		timer := time.NewTimer(w.options.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// retryAfterError preserves the handler cause while imposing a durable job's
// retry floor on the queue's independently configured exponential backoff.
type retryAfterError struct {
	cause error
	delay time.Duration
}

func (e *retryAfterError) Error() string { return "delivery requires delayed retry" }
func (e *retryAfterError) Unwrap() error { return e.cause }
