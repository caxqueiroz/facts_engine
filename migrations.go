package factsengine

import (
	"embed"
	"io/fs"
)

//go:embed sql/migrations/*.sql
var migrationFiles embed.FS

// Migrations returns the goose SQL migrations for the ledger schema. Hosts run
// them with their own goose provider, for example with a separate version table.
func Migrations() fs.FS {
	sub, err := fs.Sub(migrationFiles, "sql/migrations")
	if err != nil {
		// fs.Sub only fails for an invalid path, and the path is a constant.
		panic(err)
	}
	return sub
}
