package workers

import (
	"context"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"sync"
	"testing"
	"time"
)

type queueFake struct {
	model.Outbox
	mu                       sync.Mutex
	renewError               error
	renewals, acks, failures int
	delay                    time.Duration
}

func (q *queueFake) ClaimDelivery(context.Context, model.ConsumerKey, time.Duration) (model.Delivery, error) {
	return model.Delivery{Attempt: 3}, nil
}
func (q *queueFake) RenewDelivery(context.Context, model.Delivery, time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.renewals++
	return q.renewError
}
func (q *queueFake) AcknowledgeDelivery(context.Context, model.Delivery) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acks++
	return nil
}
func (q *queueFake) FailDelivery(_ context.Context, _ model.Delivery, d time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.failures++
	q.delay = d
	return nil
}
func workerFor(t *testing.T, q *queueFake, h HandlerFunc) *Worker {
	t.Helper()
	w, err := NewOutbox(q, model.ConsumerKey{Tenant: "tenant", Consumer: "consumer"}, h, Options{Lease: 3 * time.Second, PollInterval: time.Millisecond, MinBackoff: time.Second, MaxBackoff: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	// Shorten only the test heartbeat interval; public configuration is validated.
	w.options.Lease = 30 * time.Millisecond
	return w
}
func TestRenewalAndLeaseLoss(t *testing.T) {
	for _, lose := range []bool{false, true} {
		t.Run(map[bool]string{false: "renew", true: "lease_loss"}[lose], func(t *testing.T) {
			q := &queueFake{}
			if lose {
				q.renewError = model.ErrRevision
			}
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			w := workerFor(t, q, func(ctx context.Context, _ model.Delivery) error {
				if lose {
					<-ctx.Done()
					return ctx.Err()
				}
				// Wait until a renewal has actually happened, without timing assumptions.
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-tick.C:
						q.mu.Lock()
						n := q.renewals
						q.mu.Unlock()
						if n > 0 {
							return nil
						}
					}
				}
			})
			worked, err := w.ProcessOne(ctx)
			if !worked {
				t.Fatal("no delivery")
			}
			q.mu.Lock()
			defer q.mu.Unlock()
			if lose {
				if !errors.Is(err, model.ErrRevision) || q.acks != 0 || q.failures != 0 {
					t.Fatal(err, q)
				}
			} else {
				if err != nil || q.acks != 1 || q.renewals < 1 {
					t.Fatal(err, q)
				}
			}
		})
	}
}
func TestCancellationLeavesDeliveryUnacknowledged(t *testing.T) {
	q := &queueFake{}
	ctx, cancel := context.WithCancel(context.Background())
	w := workerFor(t, q, func(ctx context.Context, _ model.Delivery) error { cancel(); <-ctx.Done(); return ctx.Err() })
	worked, err := w.ProcessOne(ctx)
	if !worked || !errors.Is(err, context.Canceled) || q.acks != 0 || q.failures != 0 {
		t.Fatal(worked, err, q)
	}
}
func TestHandlerFailureBackoff(t *testing.T) {
	q := &queueFake{}
	w := workerFor(t, q, func(context.Context, model.Delivery) error {
		return errors.New("private external error must not be stored")
	})
	worked, err := w.ProcessOne(context.Background())
	if !worked || err != nil || q.acks != 0 || q.failures != 1 || q.delay < 2*time.Second || q.delay > 4*time.Second {
		t.Fatal(worked, err, q)
	}
	for _, attempt := range []int{1, 2, 100} {
		w.options.MinBackoff = time.Millisecond
		w.options.MaxBackoff = 5 * time.Millisecond
		if d := w.backoff(attempt); d < time.Millisecond || d > 5*time.Millisecond {
			t.Fatal(d)
		}
	}
}
