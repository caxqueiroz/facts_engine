package cortex

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/model"
	"math"
	"sort"
)

func (i *Index) SetProjectionGate(ctx context.Context, k model.GenerationKey, fence int64, sealed bool) error {
	if k.Tenant != i.config.Tenant || k.Generation != i.config.Generation {
		return model.ErrDenied
	}
	if fence < 1 {
		return model.ErrInvalid
	}
	result, err := i.db.ExecContext(ctx, `UPDATE facts_projection_gate SET fence=$1,sealed=$2 WHERE singleton AND (fence<$1 OR (fence=$1 AND sealed=$2))`, fence, sealed)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return model.ErrRevision
	}
	return nil
}

// VerifyProjection checks every stored receipt and canonical row, not a sample.
// The sealed gate is held throughout inspection. It validates vector shape,
// not semantic embedding quality or reproducibility of an external model.
func (i *Index) VerifyProjection(ctx context.Context, k model.GenerationKey, fence int64, documents []model.ProjectionDocument) (model.ProjectionCheck, error) {
	var check model.ProjectionCheck
	if k.Tenant != i.config.Tenant || k.Generation != i.config.Generation {
		return check, model.ErrDenied
	}
	if len(documents) > 10000 {
		return check, model.ErrPrecondition
	}
	docs := append([]model.ProjectionDocument{}, documents...)
	sort.Slice(docs, func(a, b int) bool { return model.ProjectionID(docs[a]) < model.ProjectionID(docs[b]) })
	expected := make(map[string]model.ProjectionDocument, len(docs))
	size := 0
	for _, d := range docs {
		if d.Tenant != k.Tenant || d.Generation != k.Generation {
			return check, model.ErrDenied
		}
		id := model.ProjectionID(d)
		if _, ok := expected[id]; ok {
			return check, model.ErrInvalid
		}
		expected[id] = d
		b, err := json.Marshal(d)
		if err != nil {
			return check, err
		}
		size += len(b)
		if size > 64<<20 {
			return check, model.ErrPrecondition
		}
	}
	tx, err := i.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return check, err
	}
	defer tx.Rollback()
	var gateFence int64
	var sealed bool
	if err = tx.QueryRowContext(ctx, `SELECT fence,sealed FROM facts_projection_gate WHERE singleton FOR SHARE`).Scan(&gateFence, &sealed); err != nil {
		return check, err
	}
	if gateFence != fence || !sealed {
		return check, model.ErrRevision
	}
	var receipts, embeddings int
	if err = tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM facts_projection_receipts),(SELECT count(*) FROM embeddings)`).Scan(&receipts, &embeddings); err != nil {
		return check, err
	}
	if receipts != len(docs) || embeddings != len(docs) {
		return check, model.ErrPrecondition
	}
	rows, err := tx.QueryContext(ctx, `SELECT r.id,r.digest,e.content,e.metadata,e.vector::text FROM facts_projection_receipts r JOIN embeddings e ON e.id=r.id ORDER BY r.id LIMIT 10001`)
	if err != nil {
		return check, err
	}
	count := 0
	for rows.Next() {
		var id, digest, content, vector string
		var metadata []byte
		if err = rows.Scan(&id, &digest, &content, &metadata, &vector); err != nil {
			rows.Close()
			return check, err
		}
		d, ok := expected[id]
		if !ok || model.Digest(d) != digest {
			rows.Close()
			return check, model.ErrPrecondition
		}
		wantContent, wantMetadata, err := projectionContent(d)
		if err != nil {
			rows.Close()
			return check, err
		}
		var actual map[string]string
		if err = json.Unmarshal(metadata, &actual); err != nil {
			rows.Close()
			return check, err
		}
		if content != string(wantContent) || model.Digest(actual) != model.Digest(wantMetadata) {
			rows.Close()
			return check, model.ErrPrecondition
		}
		var numbers []float32
		if err = json.Unmarshal([]byte(vector), &numbers); err != nil {
			rows.Close()
			return check, err
		}
		if len(numbers) != i.config.Dimensions {
			rows.Close()
			return check, model.ErrPrecondition
		}
		nonzero := false
		for _, n := range numbers {
			if math.IsNaN(float64(n)) || math.IsInf(float64(n), 0) {
				rows.Close()
				return check, model.ErrPrecondition
			}
			nonzero = nonzero || n != 0
		}
		if !nonzero {
			rows.Close()
			return check, model.ErrPrecondition
		}
		count++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return check, err
	}
	if count != len(docs) {
		return check, model.ErrPrecondition
	}
	if err = tx.Commit(); err != nil {
		return check, err
	}
	return model.ProjectionCheck{Count: count, Digest: model.Digest(docs)}, nil
}

// Generations is an immutable host routing table. It never switches the active
// generation; managed recall reads that decision from the ledger registry.
type Generations struct {
	indexes map[model.GenerationKey]*Index
}

func NewGenerations(indexes ...*Index) (*Generations, error) {
	r := &Generations{indexes: map[model.GenerationKey]*Index{}}
	for _, i := range indexes {
		if i == nil {
			return nil, model.ErrInvalid
		}
		k := model.GenerationKey{Tenant: i.config.Tenant, Generation: i.config.Generation}
		if r.indexes[k] != nil {
			return nil, model.ErrConflict
		}
		r.indexes[k] = i
	}
	return r, nil
}
func (r *Generations) get(k model.GenerationKey) (*Index, error) {
	i := r.indexes[k]
	if i == nil {
		return nil, model.ErrUnavailable
	}
	return i, nil
}
func (r *Generations) Put(ctx context.Context, d model.ProjectionDocument) error {
	i, err := r.get(model.GenerationKey{Tenant: d.Tenant, Generation: d.Generation})
	if err != nil {
		return err
	}
	return i.Put(ctx, d)
}
func (r *Generations) Candidates(ctx context.Context, t, g, s, q string, n int) ([]model.SearchCandidate, error) {
	i, err := r.get(model.GenerationKey{Tenant: t, Generation: g})
	if err != nil {
		return nil, err
	}
	return i.Candidates(ctx, t, g, s, q, n)
}
func (r *Generations) SetProjectionGate(ctx context.Context, k model.GenerationKey, f int64, s bool) error {
	i, err := r.get(k)
	if err != nil {
		return err
	}
	return i.SetProjectionGate(ctx, k, f, s)
}
func (r *Generations) VerifyProjection(ctx context.Context, k model.GenerationKey, f int64, d []model.ProjectionDocument) (model.ProjectionCheck, error) {
	i, err := r.get(k)
	if err != nil {
		return model.ProjectionCheck{}, err
	}
	return i.VerifyProjection(ctx, k, f, d)
}
