package cortex

import (
	"context"
	"database/sql"
	_ "embed"
	"math"

	"github.com/caxqueiroz/facts_engine/model"
)

//go:embed schema/disposal_gate.sql
var disposalGateSchema string

// ProvisionDisposalGate upgrades a previously provisioned retention index with
// writers stopped, using the migration owner. Refresh role grants afterwards.
func ProvisionDisposalGate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return model.ErrInvalid
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var ready bool
	if err = tx.QueryRowContext(ctx, `SELECT to_regclass('public.facts_projection_tombstones') IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('public.facts_projection_gate') AND attname='disposal_fence' AND NOT attisdropped)`).Scan(&ready); err != nil {
		return err
	}
	if !ready {
		return model.ErrPrecondition
	}
	if _, err = tx.ExecContext(ctx, disposalGateSchema); err != nil {
		return err
	}
	return tx.Commit()
}

// SealProjectionForDisposal permanently invalidates a sealed generation's proof.
// The trusted coordinator must first establish that the ledger generation is
// retired or abandoned, and enforce authorization, retention policy and holds.
// This adapter has no ledger connection and cannot establish those conditions.
func (i *Index) SealProjectionForDisposal(ctx context.Context, k model.GenerationKey, fence int64) error {
	if k.Tenant != i.config.Tenant || k.Generation != i.config.Generation {
		return model.ErrDenied
	}
	if fence < 1 || fence == math.MaxInt64 {
		return model.ErrInvalid
	}
	tx, err := i.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE facts_projection_gate SET disposal_fence=$1,fence=$1+1 WHERE singleton AND sealed AND ((disposal_fence IS NULL AND fence=$1) OR (disposal_fence=$1 AND fence=$1+1))`, fence)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return model.ErrRevision
	}
	return tx.Commit()
}

func (i *LexicalIndex) SealProjectionForDisposal(ctx context.Context, k model.GenerationKey, fence int64) error {
	return i.index.SealProjectionForDisposal(ctx, k, fence)
}
func (r *Generations) SealProjectionForDisposal(ctx context.Context, k model.GenerationKey, fence int64) error {
	i, err := r.get(k)
	if err != nil {
		return err
	}
	return i.SealProjectionForDisposal(ctx, k, fence)
}
func (r Router) SealProjectionForDisposal(ctx context.Context, k model.GenerationKey, fence int64) error {
	i := r[k.Tenant]
	if i == nil {
		return model.ErrUnavailable
	}
	return i.SealProjectionForDisposal(ctx, k, fence)
}

var _ model.ProjectionDisposalSealer = (*Index)(nil)
