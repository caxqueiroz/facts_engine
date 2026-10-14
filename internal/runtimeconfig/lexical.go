package runtimeconfig

import (
	"context"
	"database/sql"
	"errors"
	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/model"
	_ "github.com/jackc/pgx/v5/stdlib"
	"os"
	"regexp"
	"time"
)

type LexicalRoute struct {
	Tenant         string `json:"tenant"`
	Generation     string `json:"generation"`
	DatabaseEnv    string `json:"database_env"`
	MaxConnections int    `json:"max_connections"`
}

var envName = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

func ValidateRoutes(routes []LexicalRoute) error {
	if len(routes) > 32 {
		return model.ErrInvalid
	}
	seen := map[model.GenerationKey]bool{}
	total := 0
	for _, r := range routes {
		k := model.GenerationKey{Tenant: r.Tenant, Generation: r.Generation}
		if !model.ValidID(r.Tenant) || !model.ValidID(r.Generation) || !envName.MatchString(r.DatabaseEnv) || r.MaxConnections < 1 || r.MaxConnections > 32 || seen[k] {
			return model.ErrInvalid
		}
		seen[k] = true
		total += r.MaxConnections
	}
	if total > 256 {
		return model.ErrInvalid
	}
	return nil
}

// OpenLexical never provisions a database or selects an active generation.
func OpenLexical(ctx context.Context, routes []LexicalRoute) (*cortex.Generations, func(), error) {
	if len(routes) == 0 || ValidateRoutes(routes) != nil {
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
		return nil, nil, errors.New("cannot initialize configured lexical index")
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
		index, err := cortex.New(ctx, pool, cortex.Config{Tenant: r.Tenant, Generation: r.Generation, EmbeddingID: "none", Dimensions: 1}, nil)
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
