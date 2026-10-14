// This example composes existing infrastructure. Provision definitions and OpenFGA
// relationships first. No migrations, identity inference, or workers run implicitly.
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/openfga"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "example failed; verify infrastructure and authorized configuration")
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer pool.Close()
	key, err := hex.DecodeString(os.Getenv("EVIDENCE_KEY_HEX"))
	if err != nil {
		return err
	}
	cipher, err := postgres.NewAESGCM(key)
	if err != nil {
		return err
	}
	ledger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		return err
	}
	store, err := evidence.NewPostgres(pool, cipher)
	if err != nil {
		return err
	}
	authorizer, err := openfga.New(openfga.Config{URL: os.Getenv("OPENFGA_URL"), StoreID: os.Getenv("OPENFGA_STORE_ID"), ModelID: os.Getenv("OPENFGA_MODEL_ID"), Token: os.Getenv("OPENFGA_TOKEN")})
	if err != nil {
		return err
	}
	engine, err := facts.New(ledger, authorizer, store)
	if err != nil {
		return err
	}
	// In an application, Caller comes from the trusted authentication boundary.
	caller := facts.Caller{Tenant: os.Getenv("TENANT_ID"), Principal: os.Getenv("PRINCIPAL_ID")}
	result, err := engine.GetFact(ctx, caller, facts.FactQuery{Key: model.FactKey{Scope: os.Getenv("SCOPE_ID"), Subject: os.Getenv("SUBJECT_ID"), Predicate: os.Getenv("PREDICATE")}, ValidAt: time.Now().UTC()})
	if err != nil {
		return err
	}
	fmt.Printf("resolution=%s ledger_revision=%d\n", result.State, result.LedgerRevision)
	return nil
}
