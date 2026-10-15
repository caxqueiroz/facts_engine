package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (l *Ledger) RegisterProjectionDestination(ctx context.Context, k model.GenerationKey) error {
	if !validGeneration(k) {
		return model.ErrInvalid
	}
	return l.Transact(ctx, k.Tenant, func(tx model.Tx) error {
		return ledgerdb.New(tx.(*transaction).tx).RegisterProjectionDestination(ctx, ledgerdb.RegisterProjectionDestinationParams{TenantID: k.Tenant, Generation: k.Generation, ConsumerID: model.ProjectionConsumer(k.Generation)})
	})
}

func (l *Ledger) ProjectionDestinationInventory(ctx context.Context, tenant string) (model.ProjectionDestinationInventory, error) {
	var out model.ProjectionDestinationInventory
	if !model.ValidID(tenant) {
		return out, model.ErrInvalid
	}
	err := l.Transact(ctx, tenant, func(tx model.Tx) error {
		out = model.ProjectionDestinationInventory{Destinations: []model.GenerationKey{}, MissingConsumers: []string{}}
		q := ledgerdb.New(tx.(*transaction).tx)
		generations, err := q.ListProjectionDestinations(ctx, tenant)
		if err != nil {
			return err
		}
		missing, err := q.ListUnmappedProjectionConsumers(ctx, tenant)
		if err != nil {
			return err
		}
		if len(generations) > 10000 || len(missing) > 10000 {
			return model.ErrPrecondition
		}
		for _, generation := range generations {
			out.Destinations = append(out.Destinations, model.GenerationKey{Tenant: tenant, Generation: generation})
		}
		out.MissingConsumers = missing
		out.Epoch, err = q.ProjectionRetentionEpoch(ctx, tenant)
		return err
	})
	if err != nil {
		return model.ProjectionDestinationInventory{}, err
	}
	return out, nil
}

var _ model.ProjectionDestinationRegistry = (*Ledger)(nil)
