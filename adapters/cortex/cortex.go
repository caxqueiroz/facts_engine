// Package cortex wraps the spec-pinned CortexDB PostgreSQL core. Provision is
// explicit; New never migrates, starts routines, or owns its supplied SQL pool.
package cortex

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/liliang-cn/cortexdb/v2/pkg/core"
	"math"
	"time"
)

const Version = "v2.129.0" // eaf996b62f75d5e499d1b9ca7fab470a23700385
// Config binds one separately provisioned database to exactly one tenant and
// generation. Rebuild into a new database; switching the host binding is explicit.
type Config struct {
	Tenant, Generation, EmbeddingID string
	Dimensions                      int
}

func valid(c Config) bool {
	return model.ValidID(c.Tenant) && model.ValidID(c.Generation) && model.ValidID(c.EmbeddingID) && c.Dimensions >= 1 && c.Dimensions <= 2000
}
func coreConfig(c Config) core.Config {
	return core.Config{VectorDim: c.Dimensions, IndexType: core.IndexTypeFlat, SimilarityFn: core.CosineSimilarity}
}

// Provision requires a fresh, dedicated database and privileged migration pool.
// Runtime identities must be granted only SELECT/INSERT/UPDATE on embeddings,
// SELECT on collections/identity, and SELECT/INSERT on facts_projection_receipts.
func Provision(ctx context.Context, db *sql.DB, c Config) error {
	if db == nil || !valid(c) {
		return model.ErrInvalid
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('public.facts_projection_identity') IS NOT NULL OR to_regclass('public.embeddings') IS NOT NULL OR to_regclass('public.ledger_events') IS NOT NULL`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return model.ErrPrecondition
	}
	if err := core.NewPostgresStore(db, coreConfig(c)).Init(ctx); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `CREATE TABLE facts_projection_identity(singleton boolean PRIMARY KEY CHECK(singleton),tenant text NOT NULL,generation text NOT NULL,embedding_id text NOT NULL,dimensions integer NOT NULL,cortex_version text NOT NULL); CREATE TABLE facts_projection_receipts(id text PRIMARY KEY,digest text NOT NULL); CREATE TABLE facts_projection_gate(singleton boolean PRIMARY KEY CHECK(singleton),fence bigint NOT NULL CHECK(fence>=0),sealed boolean NOT NULL); INSERT INTO facts_projection_gate VALUES(true,0,false)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO facts_projection_identity VALUES(true,$1,$2,$3,$4,$5)`, c.Tenant, c.Generation, c.EmbeddingID, c.Dimensions, Version); err != nil {
		return err
	}
	return tx.Commit()
}

type Index struct {
	db       *sql.DB
	store    *core.PostgresStore
	config   Config
	embedder model.Embedder
}

func New(ctx context.Context, db *sql.DB, c Config, embedder model.Embedder) (*Index, error) {
	if db == nil || !valid(c) || embedder == nil || db.Stats().MaxOpenConnections < 1 || db.Stats().MaxOpenConnections > 32 {
		return nil, model.ErrInvalid
	}
	var safe bool
	if err := db.QueryRowContext(ctx, `SELECT NOT rolsuper AND NOT rolbypassrls AND NOT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tableowner=current_user AND tablename IN ('embeddings','facts_projection_identity')) FROM pg_roles WHERE rolname=current_user`).Scan(&safe); err != nil {
		return nil, err
	}
	if !safe {
		return nil, model.ErrPrecondition
	}
	var tenant, generation, embedding, version string
	var dimensions int
	if err := db.QueryRowContext(ctx, `SELECT tenant,generation,embedding_id,dimensions,cortex_version FROM facts_projection_identity WHERE singleton`).Scan(&tenant, &generation, &embedding, &dimensions, &version); err != nil {
		return nil, err
	}
	if tenant != c.Tenant || generation != c.Generation || embedding != c.EmbeddingID || dimensions != c.Dimensions || version != Version {
		return nil, model.ErrPrecondition
	}
	var gate bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM facts_projection_gate WHERE singleton)`).Scan(&gate); err != nil {
		return nil, err
	}
	if !gate {
		return nil, model.ErrPrecondition
	}
	return &Index{db, core.NewPostgresStore(db, coreConfig(c)), c, embedder}, nil
}
func (i *Index) vector(ctx context.Context, text string) ([]float32, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	v, err := i.embedder.Embed(ctx, text)
	if err != nil {
		return nil, err
	}
	if len(v) != i.config.Dimensions {
		return nil, model.ErrPrecondition
	}
	nonzero := false
	for _, n := range v {
		if math.IsNaN(float64(n)) || math.IsInf(float64(n), 0) {
			return nil, model.ErrInvalid
		}
		nonzero = nonzero || n != 0
	}
	if !nonzero {
		return nil, model.ErrInvalid
	}
	return v, nil
}
func (i *Index) Put(ctx context.Context, d model.ProjectionDocument) error {
	if d.Tenant != i.config.Tenant || d.Generation != i.config.Generation {
		return model.ErrDenied
	}
	if !model.ValidID(d.RecordID) || !model.ValidID(d.VersionID) || !model.ValidID(d.Key.Scope) || d.Value.Validate() != nil {
		return model.ErrInvalid
	}
	content, metadata, err := projectionContent(d)
	if err != nil {
		return err
	}
	id, digest := model.ProjectionID(d), model.Digest(d)
	var retained string
	lookupErr := i.db.QueryRowContext(ctx, `SELECT digest FROM facts_projection_receipts WHERE id=$1`, id).Scan(&retained)
	if lookupErr == nil {
		if retained != digest {
			return model.ErrConflict
		}
		return nil
	}
	if !errors.Is(lookupErr, sql.ErrNoRows) {
		return lookupErr
	}
	// Embedding is outside the database transaction. The receipt and index write
	// share one destination transaction, independently of the source ledger.
	vector, err := i.vector(ctx, string(content))
	if err != nil {
		return err
	}
	tx, err := i.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Hold a shared gate lock through commit. Sealing waits for prior writes;
	// work that was still embedding when sealed cannot write afterward.
	var sealed bool
	if err = tx.QueryRowContext(ctx, `SELECT sealed FROM facts_projection_gate WHERE singleton FOR SHARE`).Scan(&sealed); err != nil {
		return err
	}
	if sealed {
		return model.ErrPrecondition
	}
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, id); err != nil {
		return err
	}
	var old string
	err = tx.QueryRowContext(ctx, `SELECT digest FROM facts_projection_receipts WHERE id=$1`, id).Scan(&old)
	if err == nil {
		if old != digest {
			return model.ErrConflict
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = i.store.UpsertBatchTx(ctx, tx, []*core.Embedding{{ID: id, Vector: vector, Content: string(content), Metadata: metadata}}); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO facts_projection_receipts VALUES($1,$2)`, id, digest); err != nil {
		return err
	}
	return tx.Commit()
}
func (i *Index) Candidates(ctx context.Context, tenant, generation, scope, query string, limit int) ([]model.SearchCandidate, error) {
	if tenant != i.config.Tenant || generation != i.config.Generation {
		return nil, model.ErrDenied
	}
	if !model.ValidID(scope) || len(query) == 0 || len(query) > 4096 || limit < 1 || limit > 100 {
		return nil, model.ErrInvalid
	}
	vector, err := i.vector(ctx, query)
	if err != nil {
		return nil, err
	}
	// Vector retrieval avoids upstream hybrid fallback logging and only returns
	// opaque candidate identities; the service must revalidate canonical content.
	hits, err := i.store.Search(ctx, vector, core.SearchOptions{TopK: limit, Filter: map[string]string{"tenant": tenant, "generation": generation, "scope": scope}})
	if err != nil {
		return nil, err
	}
	out := make([]model.SearchCandidate, 0, len(hits))
	for _, hit := range hits {
		out = append(out, model.SearchCandidate{RecordID: hit.Metadata["record"], VersionID: hit.Metadata["version"], Score: hit.Score})
	}
	return out, nil
}

// Close has no pool ownership or background routines to release.
func (i *Index) Close() error { return i.store.Close() }

// Router is host-provisioned; a tenant must have its own bound Index/database.
type Router map[string]*Index

func (r Router) Put(ctx context.Context, d model.ProjectionDocument) error {
	i := r[d.Tenant]
	if i == nil {
		return model.ErrUnavailable
	}
	return i.Put(ctx, d)
}
func (r Router) Candidates(ctx context.Context, t, g, s, q string, n int) ([]model.SearchCandidate, error) {
	i := r[t]
	if i == nil {
		return nil, model.ErrUnavailable
	}
	return i.Candidates(ctx, t, g, s, q, n)
}

func projectionContent(d model.ProjectionDocument) ([]byte, map[string]string, error) {
	content, err := json.Marshal(struct {
		Key   model.FactKey
		Value model.Value
	}{d.Key, d.Value})
	if err != nil {
		return nil, nil, err
	}
	if len(content) > 1<<20 {
		return nil, nil, model.ErrInvalid
	}
	metadata := map[string]string{"tenant": d.Tenant, "generation": d.Generation, "scope": d.Key.Scope, "record": d.RecordID, "version": d.VersionID, "digest": d.VersionDigest, "policy": d.PolicyID, "classification": d.Classification}
	provenance, _ := json.Marshal(struct {
		Valid         model.Interval
		SourceClasses []string
	}{d.Valid, d.SourceClasses})
	metadata["provenance"] = string(provenance)

	return content, metadata, nil
}
