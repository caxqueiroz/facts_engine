package model

import "context"

// ProjectionDestinationInventory is a point-in-time inventory of ledger-known
// destinations, including retired and abandoned generations. MissingConsumers
// lists projection consumers without a generation binding; it is not safe to
// infer their destination names from hashes. This is not disposal authorization
// or proof that a host has registered all externally provisioned indexes.
type ProjectionDestinationInventory struct {
	Destinations     []GenerationKey
	MissingConsumers []string
	Epoch            int64
}

// ProjectionDestinationRegistry is a trusted host maintenance port. Register a
// destination before writing to it; its ProjectionConsumer must already exist.
// Registration never resets consumer progress or removes old destinations.
type ProjectionDestinationRegistry interface {
	RegisterProjectionDestination(context.Context, GenerationKey) error
	ProjectionDestinationInventory(context.Context, string) (ProjectionDestinationInventory, error)
}
