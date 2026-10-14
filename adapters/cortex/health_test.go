package cortex

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestGenerationConnectivityEmptyAndClosedPool(t *testing.T) {
	empty, err := NewGenerations()
	if err != nil {
		t.Fatal(err)
	}
	if err := empty.CheckConnectivity(context.Background()); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal(err)
	}
	pool, err := sql.Open("pgx", "postgres://localhost/unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	generations, err := NewGenerations(&Index{db: pool, config: Config{Tenant: "PRIVATE-tenant", Generation: "PRIVATE-generation"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := generations.CheckConnectivity(context.Background()); err != model.ErrUnavailable {
		t.Fatal("pool error not sanitized", err)
	}
}
