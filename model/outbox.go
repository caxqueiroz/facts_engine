package model

import (
	"context"
	"time"
)

// ConsumerKey is trusted host configuration, never unchecked client input.
// Each consumer receives the complete tenant stream from revision one.
type ConsumerKey struct{ Tenant, Consumer string }
type Delivery struct {
	Key        ConsumerKey
	Event      Event
	Fence      int64
	Attempt    int
	LeaseUntil time.Time
}
type ConsumerStatus struct {
	Key                   ConsumerKey
	AcknowledgedRevision  int64
	State                 string
	Fence                 int64
	Attempts, MaxAttempts int
	LeaseUntil            *time.Time
	AvailableAt           time.Time
	LastFailureCode       string
}

// Outbox is an operational port for trusted workers. External effects must be
// idempotent by (tenant,event ID); lease fencing cannot undo external effects.
type Outbox interface {
	RegisterConsumer(context.Context, ConsumerKey, int) error
	ClaimDelivery(context.Context, ConsumerKey, time.Duration) (Delivery, error)
	RenewDelivery(context.Context, Delivery, time.Duration) error
	AcknowledgeDelivery(context.Context, Delivery) error
	FailDelivery(context.Context, Delivery, time.Duration) error
	ConsumerStatus(context.Context, ConsumerKey) (ConsumerStatus, error)
	RetryConsumer(context.Context, ConsumerKey, int64) error
}
type ChangesQuery struct {
	Scope, Purpose string
	AfterRevision  int64
	Limit          int
}
type ChangesResult struct {
	Events                        []Event
	ThroughRevision, NextRevision int64
}
type ChangesTx interface {
	Tx
	Changes(context.Context, string, int64, int64, int) ([]Event, error)
}
