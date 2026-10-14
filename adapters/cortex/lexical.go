package cortex

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

// LexicalIndex is an explicit lexical read view over a bound generation. Writes
// still use the generation's configured indexing method. No automatic switch can
// silently change the retrieval identity retained by PrepareContext.
type LexicalIndex struct{ index *Index }

func (i *Index) Lexical() *LexicalIndex { return &LexicalIndex{i} }
func (i *LexicalIndex) Put(ctx context.Context, d model.ProjectionDocument) error {
	return i.index.Put(ctx, d)
}
func (i *LexicalIndex) Candidates(ctx context.Context, tenant, generation, scope, query string, limit int) ([]model.SearchCandidate, error) {
	return i.index.lexicalCandidates(ctx, tenant, generation, scope, query, limit)
}
func (i *LexicalIndex) DescribeRetrieval(_ context.Context, tenant, generation string) (model.RetrievalIdentity, error) {
	if tenant != i.index.config.Tenant || generation != i.index.config.Generation {
		return model.RetrievalIdentity{}, model.ErrDenied
	}
	return lexicalIdentity(generation), nil
}
func lexicalIdentity(generation string) model.RetrievalIdentity {
	return model.RetrievalIdentity{Generation: generation, EmbeddingID: "none", Dimensions: 0, SearchVersion: "cortex-" + Version + "-postgres-fts-simple-v1", RerankerID: "none"}
}
func (i *Index) lexicalCandidates(ctx context.Context, tenant, generation, scope, query string, limit int) ([]model.SearchCandidate, error) {
	if tenant != i.config.Tenant || generation != i.config.Generation {
		return nil, model.ErrDenied
	}
	if !model.ValidID(scope) || len(query) == 0 || len(query) > 4096 || limit < 1 || limit > 100 {
		return nil, model.ErrInvalid
	}
	// Explicit regconfig makes results independent of session language settings;
	// plainto_tsquery treats caller text as data rather than query syntax.
	rows, err := i.db.QueryContext(ctx, `SELECT metadata->>'record',metadata->>'version',coalesce(metadata->>'kind',''),ts_rank_cd(to_tsvector('simple',content),plainto_tsquery('simple',$4)) AS score FROM embeddings WHERE metadata->>'tenant'=$1 AND metadata->>'generation'=$2 AND metadata->>'scope'=$3 AND to_tsvector('simple',content) @@ plainto_tsquery('simple',$4) ORDER BY score DESC,metadata->>'version',id LIMIT $5`, tenant, generation, scope, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.SearchCandidate{}
	for rows.Next() {
		var hit model.SearchCandidate
		if err = rows.Scan(&hit.RecordID, &hit.VersionID, &hit.Kind, &hit.Score); err != nil {
			return nil, err
		}
		out = append(out, hit)
	}
	return out, rows.Err()
}
