package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) commitTransaction(ctx context.Context, tenant string, fn func(context.Context, model.Tx) error) (err error) {
	ctx, observation := s.telemetry.Start(ctx, "commit.transaction")
	defer observation.End(&err)
	return s.ledger.Transact(ctx, tenant, func(tx model.Tx) error { return fn(ctx, tx) })
}
