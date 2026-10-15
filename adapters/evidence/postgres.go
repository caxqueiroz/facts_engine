// Package evidence provides encrypted development evidence storage.
package evidence

import (
	"context"
	"encoding/json"
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
	return s.put(ctx, tenant, id, payload, nil)
}

// PutPending records a new object's upload scope, orphan policy and database
// timestamp atomically with its bytes. The caller owns upload authorization.
// Existing objects without this provenance cannot be adopted retrospectively.
func (s *Postgres) PutPending(ctx context.Context, tenant string, upload model.PendingEvidenceUpload, payload []byte) (model.ObjectRef, error) {
	if !model.ValidID(upload.Scope) || !model.ValidID(upload.PolicyID) {
		return model.ObjectRef{}, model.ErrInvalid
	}
	return s.put(ctx, tenant, upload.ID, payload, &upload)
}

func (s *Postgres) put(ctx context.Context, tenant, id string, payload []byte, upload *model.PendingEvidenceUpload) (model.ObjectRef, error) {
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
		if upload != nil {
			if err := s.recordPendingUpload(ctx, tx, tenant, ref, *upload); err != nil {
				return err
			}
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

func (s *Postgres) recordPendingUpload(ctx context.Context, tx pgx.Tx, tenant string, ref model.ObjectRef, upload model.PendingEvidenceUpload) error {
	queries := ledgerdb.New(tx)
	retained, err := queries.GetPendingEvidenceUpload(ctx, ledgerdb.GetPendingEvidenceUploadParams{TenantID: tenant, Locator: ref.Locator, ObjectVersion: ref.Version})
	if err == nil {
		if retained.ScopeID != upload.Scope || retained.PolicyID != upload.PolicyID || retained.ByteSize != ref.Size {
			return model.ErrConflict
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	_, err = queries.GetEvidenceObjectPayload(ctx, ledgerdb.GetEvidenceObjectPayloadParams{TenantID: tenant, Locator: ref.Locator, ObjectVersion: ref.Version})
	if err == nil {
		return model.ErrPrecondition
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	encrypted, err := queries.GetRetentionPolicyPayload(ctx, ledgerdb.GetRetentionPolicyPayloadParams{TenantID: tenant, ScopeID: upload.Scope, ID: upload.PolicyID})
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrPrecondition
	}
	if err != nil {
		return err
	}
	raw, err := s.cipher.Open(tenant, encrypted)
	if err != nil {
		return err
	}
	var policy model.RetentionPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return err
	}
	if policy.Scope != upload.Scope || policy.ID != upload.PolicyID || policy.RecordClass != "orphan_evidence_bytes" {
		return model.ErrPrecondition
	}
	return queries.InsertPendingEvidenceUpload(ctx, ledgerdb.InsertPendingEvidenceUploadParams{TenantID: tenant, Locator: ref.Locator, ObjectVersion: ref.Version, ScopeID: upload.Scope, PolicyID: upload.PolicyID, ByteSize: ref.Size})
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
