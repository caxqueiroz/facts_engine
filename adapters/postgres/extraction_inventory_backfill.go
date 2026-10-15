package postgres

import (
	"context"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/internal/pgcleanup"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
)

// ExtractionInventoryBackfill reports a committed page. Complete means no
// missing inventory receipts were observed in that tenant at the final query;
// it is not authorization for disposal or verification of all retained copies.
type ExtractionInventoryBackfill struct {
	Inventoried int
	Complete    bool
}

// BackfillExtractionInputInventories is a host-authorized maintenance operation,
// not an engine command or public RPC. Upgrade and stop legacy extraction writers
// first. The host owns tenant authorization, pool credentials and decryption keys.
// Each call processes at most limit attempts (1..100) and 64 MiB of ciphertext.
// It never rewrites an attempt or a completed inventory. Malformed input, missing
// source content, or partial inventory rows fail the entire page atomically.
// Repeating calls drains missing receipts without requiring a persistent cursor.
func (l *Ledger) BackfillExtractionInputInventories(ctx context.Context, tenant string, limit int) (result ExtractionInventoryBackfill, err error) {
	if !model.ValidID(tenant) || limit < 1 || limit > 100 {
		return result, model.ErrInvalid
	}
	// Inputs are immutable. Read committed lets serialized backfill calls observe
	// receipts committed by a previous owner after waiting for the advisory lock.
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return result, translate(err)
	}
	defer pgcleanup.Rollback(ctx, tx.Rollback)
	q := ledgerdb.New(tx)
	if err := q.BindTenant(ctx, tenant); err != nil {
		return result, translate(err)
	}
	lock := model.Digest([]string{"extraction-input-inventory-backfill-v1", tenant})
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lock); err != nil {
		return result, translate(err)
	}
	rows, err := tx.Query(ctx, ledgerdb.ListUninventoriedExtractionAttemptPage, tenant, int32(limit))
	if err != nil {
		return result, translate(err)
	}
	attempts := []model.ExtractionAttempt{}
	total := 0
	t := &transaction{tx: tx, tenant: tenant, cipher: l.cipher}
	for rows.Next() {
		var id, job, scope string
		var payload []byte
		if err := rows.Scan(&id, &job, &scope, &payload); err != nil {
			rows.Close()
			return result, err
		}
		total += len(payload)
		if total > 64<<20 {
			rows.Close()
			return result, model.ErrPrecondition
		}
		var a model.ExtractionAttempt
		if err := t.decode(payload, &a); err != nil {
			rows.Close()
			return result, err
		}
		if a.Tenant != tenant || a.ID != id || a.JobID != job || a.Scope != scope {
			rows.Close()
			return result, model.ErrPrecondition
		}
		if _, _, err := extractionInputReferences(a); err != nil {
			rows.Close()
			return result, err
		}
		attempts = append(attempts, a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, translate(err)
	}
	for _, a := range attempts {
		partial, err := q.HasPartialExtractionInputInventory(ctx, ledgerdb.HasPartialExtractionInputInventoryParams{TenantID: tenant, AttemptID: a.ID})
		if err != nil {
			return result, translate(err)
		}
		if !partial.Valid || partial.Bool {
			return result, model.ErrPrecondition
		}
		if err := t.retainExtractionInputInventory(ctx, a); err != nil {
			return result, translate(err)
		}
	}
	missing, err := q.HasUninventoriedExtractionAttempts(ctx, tenant)
	if err != nil {
		return result, translate(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return result, translate(err)
	}
	return ExtractionInventoryBackfill{Inventoried: len(attempts), Complete: !missing}, nil
}
