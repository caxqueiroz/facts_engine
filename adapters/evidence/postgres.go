// Package evidence provides encrypted development evidence storage.
package evidence

import (
	"context"
	"errors"
	"fmt"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/internal/pgcleanup"
	"github.com/caxqueiroz/facts_engine/internal/pgfailure"
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
func (s *Postgres) transaction(ctx context.Context, tenant string, fn func(pgx.Tx) error) (err error) {
	defer func() { err = pgfailure.Normalize(err) }()
	if !model.ValidID(tenant) {
		return model.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer pgcleanup.Rollback(ctx, tx.Rollback)
	if err = ledgerdb.New(tx).BindTenant(ctx, tenant); err != nil {
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
		disposed, err := ledgerdb.New(tx).LockRetentionObject(ctx, ledgerdb.LockRetentionObjectParams{TenantID: tenant, Locator: id, ObjectVersion: ref.Version})
		if err != nil {
			return err
		}
		if disposed {
			return model.ErrPrecondition
		}
		err = ledgerdb.New(tx).InsertEvidenceObject(ctx, ledgerdb.InsertEvidenceObjectParams{TenantID: tenant, Locator: id, ObjectVersion: ref.Version, Payload: b})
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var b []byte
	err := s.transaction(ctx, tenant, func(tx pgx.Tx) error {
		var err error
		b, err = ledgerdb.New(tx).GetEvidenceObjectPayload(ctx, ledgerdb.GetEvidenceObjectPayloadParams{TenantID: tenant, Locator: ref.Locator, ObjectVersion: ref.Version})
		return err
	})
	if interrupted := ctx.Err(); interrupted != nil {
		return nil, interrupted
	}
	if errors.Is(err, context.Canceled) {
		return nil, context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, context.DeadlineExceeded
	}
	if errors.Is(err, model.ErrUnavailable) {
		return nil, model.ErrUnavailable
	}
	if err != nil {
		return nil, fmt.Errorf("evidence unavailable: %w", model.ErrPrecondition)
	}
	p, err := s.cipher.Open(tenant, b)
	if interrupted := ctx.Err(); interrupted != nil {
		return nil, interrupted
	}
	if err != nil {
		return nil, err
	}
	if model.BytesDigest(p) != ref.Digest || int64(len(p)) != ref.Size {
		return nil, model.ErrPrecondition
	}
	return p, nil
}

func (s *Postgres) RetentionDomain(ctx context.Context) (string, error) {
	id, err := ledgerdb.New(s.pool).GetRetentionDomain(ctx)
	return id, err
}
