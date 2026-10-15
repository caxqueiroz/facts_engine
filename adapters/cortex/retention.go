package cortex

import (
	"context"
	"database/sql"
	_ "embed"

	"github.com/caxqueiroz/facts_engine/model"
)

//go:embed schema/retention.sql
var retentionSchema string

// ProvisionRetention upgrades an existing dedicated index database once, using
// an owner connection with writers stopped. New databases receive this schema
// through Provision. Refresh runtime/retention grants before restarting writers.
func ProvisionRetention(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return model.ErrInvalid
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var ready bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('public.facts_projection_identity') IS NOT NULL AND to_regclass('public.facts_projection_gate') IS NOT NULL AND to_regclass('public.facts_projection_tombstones') IS NULL`).Scan(&ready); err != nil {
		return err
	}
	if !ready {
		return model.ErrPrecondition
	}
	if _, err := tx.ExecContext(ctx, retentionSchema); err != nil {
		return err
	}
	return tx.Commit()
}

func projectionDisposalID(r model.ProjectionDisposal) (string, error) {
	if !model.ValidID(r.Tenant) || !model.ValidID(r.Generation) || !model.ValidID(r.VersionID) || (r.Kind != "fact" && r.Kind != "memory") {
		return "", model.ErrInvalid
	}
	d := model.ProjectionDocument{Tenant: r.Tenant, Generation: r.Generation, VersionID: r.VersionID}
	if r.Kind == "memory" {
		d.Memory = &model.MemoryProjection{}
	}
	return model.ProjectionID(d), nil
}

func (i *Index) DisposeProjection(ctx context.Context, r model.ProjectionDisposal) error {
	if r.Tenant != i.config.Tenant || r.Generation != i.config.Generation {
		return model.ErrDenied
	}
	id, err := projectionDisposalID(r)
	if err != nil {
		return err
	}
	tx, err := i.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var sealed bool
	var disposalFence sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT sealed,disposal_fence FROM facts_projection_gate WHERE singleton FOR SHARE`).Scan(&sealed, &disposalFence); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, id); err != nil {
		return err
	}
	var disposed bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM facts_projection_tombstones WHERE id=$1)`, id).Scan(&disposed); err != nil {
		return err
	}
	if disposed {
		return tx.Commit()
	}
	if sealed && !disposalFence.Valid {
		return model.ErrPrecondition
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO facts_projection_tombstones(id) VALUES($1)`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM embeddings WHERE id=$1`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM facts_projection_receipts WHERE id=$1`, id); err != nil {
		return err
	}
	return tx.Commit()
}
func (i *LexicalIndex) DisposeProjection(ctx context.Context, r model.ProjectionDisposal) error {
	return i.index.DisposeProjection(ctx, r)
}
func (r *Generations) DisposeProjection(ctx context.Context, d model.ProjectionDisposal) error {
	i, err := r.get(model.GenerationKey{Tenant: d.Tenant, Generation: d.Generation})
	if err != nil {
		return err
	}
	return i.DisposeProjection(ctx, d)
}
func (r Router) DisposeProjection(ctx context.Context, d model.ProjectionDisposal) error {
	index := r[d.Tenant]
	if index == nil {
		return model.ErrUnavailable
	}
	return index.DisposeProjection(ctx, d)
}

// NewRetention opens an existing destination for cleanup without configuring an
// embedding provider. Identity, schema, role and pool checks match New. The
// caller supplies separately provisioned retention credentials; this does not
// grant privileges or migrate the index.
func NewRetention(ctx context.Context, db *sql.DB, config Config) (*Index, error) {
	var embedder model.Embedder
	if config.EmbeddingID != "none" {
		embedder = retentionOnlyEmbedder{}
	}
	return New(ctx, db, config, embedder)
}

type retentionOnlyEmbedder struct{}

func (retentionOnlyEmbedder) Embed(context.Context, string) ([]float32, error) {
	return nil, model.ErrPrecondition
}
