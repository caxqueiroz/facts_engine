package factsengine_test

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	facts "github.com/caxqueiroz/facts_engine"
)

func TestMigrationsMatchRepository(t *testing.T) {
	entries, err := os.ReadDir("sql/migrations")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			want = append(want, e.Name())
		}
	}
	if len(want) == 0 {
		t.Fatal("no migrations in sql/migrations")
	}
	embedded, err := fs.ReadDir(facts.Migrations(), ".")
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, e := range embedded {
		got = append(got, e.Name())
	}
	if !slices.Equal(got, want) {
		t.Fatalf("embedded migrations = %v, want %v", got, want)
	}
	for _, name := range want {
		disk, err := os.ReadFile(filepath.Join("sql/migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		b, err := fs.ReadFile(facts.Migrations(), name)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, disk) {
			t.Fatalf("embedded %s differs from the repository file", name)
		}
	}
}
