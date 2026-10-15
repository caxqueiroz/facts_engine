package runtimeconfig

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"time"

	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/model"
)

type CleanupRoute struct {
	LexicalRoute
	EmbeddingID string `json:"embedding_id"`
	Dimensions  int    `json:"dimensions"`
}

func ValidateCleanupRoutes(routes []CleanupRoute) error {
	if len(routes) == 0 {
		return model.ErrInvalid
	}
	base := make([]LexicalRoute, 0, len(routes))
	for _, r := range routes {
		if !model.ValidID(r.EmbeddingID) || r.Dimensions < 1 || r.Dimensions > 2000 || (r.EmbeddingID == "none" && r.Dimensions != 1) {
			return model.ErrInvalid
		}
		base = append(base, r.LexicalRoute)
	}
	return ValidateRoutes(base)
}

// OpenCleanup binds existing destination identities without an embedding
// provider, provisioning, consumer registration or selection of active indexes.
func OpenCleanup(ctx context.Context, routes []CleanupRoute) (*cortex.Generations, func(), error) {
	if ValidateCleanupRoutes(routes) != nil {
		return nil, nil, model.ErrInvalid
	}
	var pools []*sql.DB
	var indexes []*cortex.Index
	closePools := func() {
		for _, pool := range pools {
			_ = pool.Close()
		}
	}
	fail := func() (*cortex.Generations, func(), error) {
		closePools()
		return nil, nil, errors.New("cannot initialize configured cleanup index")
	}
	for _, r := range routes {
		dsn := os.Getenv(r.DatabaseEnv)
		if dsn == "" {
			return fail()
		}
		pool, err := sql.Open("pgx", dsn)
		if err != nil {
			return fail()
		}
		pools = append(pools, pool)
		pool.SetMaxOpenConns(r.MaxConnections)
		pool.SetMaxIdleConns(min(r.MaxConnections, 2))
		pool.SetConnMaxLifetime(30 * time.Minute)
		index, err := cortex.NewRetention(ctx, pool, cortex.Config{Tenant: r.Tenant, Generation: r.Generation, EmbeddingID: r.EmbeddingID, Dimensions: r.Dimensions})
		if err != nil {
			return fail()
		}
		indexes = append(indexes, index)
	}
	index, err := cortex.NewGenerations(indexes...)
	if err != nil {
		return fail()
	}
	return index, closePools, nil
}
