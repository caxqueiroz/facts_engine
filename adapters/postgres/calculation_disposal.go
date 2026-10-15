package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) guardCalculationContent(ctx context.Context, id string) error {
	q := ledgerdb.New(t.tx)
	if err := q.LockCalculationGuard(ctx, ledgerdb.LockCalculationGuardParams{TenantID: t.tenant, CalculationID: id}); err != nil {
		return err
	}
	available, err := q.CalculationContentAvailable(ctx, ledgerdb.CalculationContentAvailableParams{TenantID: t.tenant, CalculationID: id})
	if err != nil {
		return err
	}
	if !available.Valid || !available.Bool {
		return model.ErrPrecondition
	}
	return nil
}
