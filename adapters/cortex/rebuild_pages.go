package cortex

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"

	"github.com/caxqueiroz/facts_engine/model"
)

func (i *Index) verifyProjectionRow(d model.ProjectionDocument, digest, content string, metadata []byte, vector string) error {
	if model.Digest(d) != digest {
		return model.ErrPrecondition
	}
	wantContent, wantMetadata, err := projectionContent(d)
	if err != nil {
		return err
	}
	var actual map[string]string
	if err = json.Unmarshal(metadata, &actual); err != nil {
		return err
	}
	if content != string(wantContent) || model.Digest(actual) != model.Digest(wantMetadata) {
		return model.ErrPrecondition
	}
	var numbers []float32
	if err = json.Unmarshal([]byte(vector), &numbers); err != nil {
		return err
	}
	if len(numbers) != i.config.Dimensions {
		return model.ErrPrecondition
	}
	nonzero := false
	for _, n := range numbers {
		if math.IsNaN(float64(n)) || math.IsInf(float64(n), 0) {
			return model.ErrPrecondition
		}
		nonzero = nonzero || n != 0
	}
	if !nonzero {
		return model.ErrPrecondition
	}
	return nil
}

// VerifyProjectionPages retains the sealed gate lock and one destination
// snapshot across all source pages. Exact cardinalities plus unique expected IDs
// rule out missing rows, extra rows and orphan receipts without an in-memory set
// of the entire inventory.
func (i *Index) VerifyProjectionPages(ctx context.Context, k model.GenerationKey, fence int64, load model.ProjectionPageLoader) (model.ProjectionCheck, error) {
	empty := model.ProjectionCheck{}
	if k.Tenant != i.config.Tenant || k.Generation != i.config.Generation {
		return empty, model.ErrDenied
	}
	if fence < 1 || load == nil {
		return empty, model.ErrInvalid
	}
	tx, err := i.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	var gateFence int64
	var sealed bool
	var disposalFence sql.NullInt64
	if err = tx.QueryRowContext(ctx, `SELECT fence,sealed,disposal_fence FROM facts_projection_gate WHERE singleton FOR SHARE`).Scan(&gateFence, &sealed, &disposalFence); err != nil {
		return empty, err
	}
	if gateFence != fence || !sealed || disposalFence.Valid {
		return empty, model.ErrRevision
	}
	var receipts, embeddings int
	if err = tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM facts_projection_receipts),(SELECT count(*) FROM embeddings)`).Scan(&receipts, &embeddings); err != nil {
		return empty, err
	}
	if receipts != embeddings {
		return empty, model.ErrPrecondition
	}
	inventory := model.NewProjectionInventory()
	cursor := model.ProjectionCursor{}
	for {
		if err = ctx.Err(); err != nil {
			return empty, err
		}
		page, err := load(ctx, cursor)
		if err != nil {
			return empty, err
		}
		if model.ValidateProjectionPage(page, cursor) != nil {
			return empty, model.ErrInvalid
		}
		ids := make([]string, 0, len(page.Documents))
		expected := make(map[string]model.ProjectionDocument, len(page.Documents))
		size := 0
		for _, d := range page.Documents {
			if d.Tenant != k.Tenant || d.Generation != k.Generation {
				return empty, model.ErrDenied
			}
			if err = inventory.Add(d); err != nil {
				return empty, err
			}
			payload, err := json.Marshal(d)
			if err != nil {
				return empty, err
			}
			size += len(payload)
			if size > model.MaxProjectionPageBytes {
				return empty, model.ErrPrecondition
			}
			id := model.ProjectionID(d)
			ids = append(ids, id)
			expected[id] = d
		}
		if len(ids) > 0 {
			if err = i.verifyProjectionPageRows(ctx, tx, ids, expected); err != nil {
				return empty, err
			}
		}
		if inventory.Check().Count > receipts {
			return empty, model.ErrPrecondition
		}
		if page.Next == nil {
			break
		}
		cursor = *page.Next
	}
	check := inventory.Check()
	if check.Count != receipts {
		return empty, model.ErrPrecondition
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	return check, nil
}

func (i *Index) verifyProjectionPageRows(ctx context.Context, tx *sql.Tx, ids []string, expected map[string]model.ProjectionDocument) error {
	rows, err := tx.QueryContext(ctx, `SELECT r.id,r.digest,e.content,e.metadata,e.vector::text FROM facts_projection_receipts r JOIN embeddings e ON e.id=r.id WHERE r.id=ANY($1::text[])`, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id, digest, content, vector string
		var metadata []byte
		if err = rows.Scan(&id, &digest, &content, &metadata, &vector); err != nil {
			return err
		}
		d, ok := expected[id]
		if !ok {
			return model.ErrPrecondition
		}
		if err = i.verifyProjectionRow(d, digest, content, metadata, vector); err != nil {
			return err
		}
		count++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if count != len(ids) {
		return model.ErrPrecondition
	}
	return nil
}

func (r *Generations) VerifyProjectionPages(ctx context.Context, k model.GenerationKey, fence int64, load model.ProjectionPageLoader) (model.ProjectionCheck, error) {
	index, err := r.get(k)
	if err != nil {
		return model.ProjectionCheck{}, err
	}
	return index.VerifyProjectionPages(ctx, k, fence, load)
}

var _ model.PagedRebuildIndex = (*Index)(nil)
var _ model.PagedRebuildIndex = (*Generations)(nil)
