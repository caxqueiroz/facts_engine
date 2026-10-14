// Package evidence provides encrypted development evidence storage.
package evidence

import (
	"context"
	"fmt"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool   *pgxpool.Pool
	cipher postgres.Cipher
}

func NewPostgres(pool *pgxpool.Pool, cipher postgres.Cipher) (*Postgres, error) {
	if pool == nil || cipher == nil {
		return nil, model.ErrInvalid
	}
	return &Postgres{pool, cipher}, nil
}
func (s *Postgres) transaction(ctx context.Context, tenant string, fn func(pgx.Tx) error) error {
	if !model.ValidID(tenant) {
		return model.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err = tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true)`, tenant); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Postgres) Put(ctx context.Context, tenant, id string, payload []byte) (model.ObjectRef, error) {
	if !model.ValidID(id) || len(payload) > 16<<20 {
		return model.ObjectRef{}, model.ErrInvalid
	}
	ref := model.ObjectRef{Locator: id, Version: model.BytesDigest(payload), Digest: model.BytesDigest(payload), Size: int64(len(payload))}
	b, err := s.cipher.Seal(tenant, payload)
	if err != nil {
		return ref, err
	}
	err = s.transaction(ctx, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO evidence_objects VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, tenant, id, ref.Version, b)
		return err
	})
	if err != nil {
		return ref, err
	}
	stored, err := s.Read(ctx, tenant, ref)
	if err != nil {
		return ref, err
	}
	if model.BytesDigest(stored) != ref.Digest {
		return ref, model.ErrPrecondition
	}
	return ref, nil
}
func (s *Postgres) Read(ctx context.Context, tenant string, ref model.ObjectRef) ([]byte, error) {
	var b []byte
	err := s.transaction(ctx, tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT payload FROM evidence_objects WHERE tenant_id=$1 AND locator=$2 AND object_version=$3`, tenant, ref.Locator, ref.Version).Scan(&b)
	})
	if err != nil {
		return nil, fmt.Errorf("evidence unavailable: %w", model.ErrPrecondition)
	}
	p, err := s.cipher.Open(tenant, b)
	if err != nil {
		return nil, err
	}
	if model.BytesDigest(p) != ref.Digest || int64(len(p)) != ref.Size {
		return nil, model.ErrPrecondition
	}
	return p, nil
}
