package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgtype"
)

func (l *Ledger) ProjectionCleanupStatus(ctx context.Context, k model.ProjectionCleanupKey) (model.ProjectionCleanupStatus, error) {
	var out model.ProjectionCleanupStatus
	if !model.ValidID(k.Tenant) || !model.ValidID(k.VersionID) || (k.Kind != "fact" && k.Kind != "memory") {
		return out, model.ErrInvalid
	}
	err := l.Transact(ctx, k.Tenant, func(tx model.Tx) error {
		out = model.ProjectionCleanupStatus{Key: k, Targets: []model.ProjectionCleanupTarget{}, Complete: true}
		q := ledgerdb.New(tx.(*transaction).tx)
		job, err := q.GetProjectionCleanupJob(ctx, ledgerdb.GetProjectionCleanupJobParams{TenantID: k.Tenant, Kind: k.Kind, VersionID: k.VersionID})
		if err != nil {
			return translate(err)
		}
		if job.InventoryEpoch < 1 || !job.CreatedAt.Valid || job.CreatedAt.InfinityModifier != pgtype.Finite {
			return model.ErrPrecondition
		}
		out.InventoryEpoch, out.CreatedAt = job.InventoryEpoch, job.CreatedAt.Time
		targets, err := q.ListProjectionCleanupTargets(ctx, ledgerdb.ListProjectionCleanupTargetsParams{TenantID: k.Tenant, Kind: k.Kind, VersionID: k.VersionID})
		if err != nil {
			return err
		}
		if len(targets) > 10000 || len(targets) != int(job.TargetCount) {
			return model.ErrPrecondition
		}
		for _, target := range targets {
			entry := model.ProjectionCleanupTarget{Generation: target.Generation}
			if target.AcknowledgementID.Valid != target.CompletedAt.Valid {
				return model.ErrPrecondition
			}
			if target.CompletedAt.Valid {
				if target.CompletedAt.InfinityModifier != pgtype.Finite || !model.ValidID(target.AcknowledgementID.String) {
					return model.ErrPrecondition
				}
				entry.AcknowledgementID = target.AcknowledgementID.String
				at := target.CompletedAt.Time
				entry.CompletedAt = &at
			} else {
				out.Complete = false
			}
			out.Targets = append(out.Targets, entry)
		}
		return nil
	})
	if err != nil {
		return model.ProjectionCleanupStatus{}, err
	}
	return out, nil
}

var _ model.ProjectionCleanupReader = (*Ledger)(nil)
