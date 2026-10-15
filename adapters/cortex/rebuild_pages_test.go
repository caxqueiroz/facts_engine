package cortex_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/adapters/cortex"
	"github.com/caxqueiroz/facts_engine/model"
)

func testPagedProjectionVerification(t *testing.T, ctx context.Context, index *cortex.Index, admin *sql.DB) {
	t.Helper()
	const count = 10005
	body := strings.Repeat("accepted synthetic text ", 320)
	document := func(n int) model.ProjectionDocument {
		return model.ProjectionDocument{Tenant: "bank-b", Generation: "g1", RecordID: "record", VersionID: fmt.Sprintf("v%05d", n), VersionDigest: model.Digest(n), PolicyID: "policy", Classification: "confidential", Key: model.FactKey{Scope: "case", Subject: "subject", Predicate: "predicate"}, Value: model.Value{Kind: model.Text, Text: &body}}
	}
	first := document(0)
	serialized, _ := json.Marshal(first)
	if len(serialized)*count <= 64<<20 {
		t.Fatal("fixture must exceed the old payload limit")
	}
	if err := index.Put(ctx, first); err != nil {
		t.Fatal(err)
	}
	var collection, content string
	var metadata []byte
	if err := admin.QueryRowContext(ctx, `SELECT collection_id,content,metadata FROM embeddings WHERE id=$1`, model.ProjectionID(first)).Scan(&collection, &content, &metadata); err != nil {
		t.Fatal(err)
	}
	// Seed a large synthetic destination through bounded batches. The first row
	// comes from Put so the fixture retains the actual projection envelope.
	tx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for start := 1; start < count; start += 100 {
		type row struct {
			ID       string            `json:"id"`
			Digest   string            `json:"digest"`
			Metadata map[string]string `json:"metadata"`
		}
		batch := []row{}
		for n := start; n < min(start+100, count); n++ {
			d := document(n)
			var values map[string]string
			if err = json.Unmarshal(metadata, &values); err != nil {
				t.Fatal(err)
			}
			values["version"] = d.VersionID
			values["digest"] = d.VersionDigest
			batch = append(batch, row{model.ProjectionID(d), model.Digest(d), values})
		}
		raw, err := json.Marshal(batch)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO embeddings(id,collection_id,vector,content,metadata) SELECT r.id,$2,'[1,0,0]'::vector,$3,r.metadata FROM jsonb_to_recordset($1::jsonb) AS r(id text,digest text,metadata jsonb)`, string(raw), collection, content); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO facts_projection_receipts SELECT r.id,r.digest FROM jsonb_to_recordset($1::jsonb) AS r(id text,digest text)`, string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	key := model.GenerationKey{Tenant: "bank-b", Generation: "g1"}
	if err = index.SetProjectionGate(ctx, key, 1, true); err != nil {
		t.Fatal(err)
	}
	loader := func(_ context.Context, after model.ProjectionCursor) (model.ProjectionPage, error) {
		start := 0
		if after.VersionID != "" {
			n, err := strconv.Atoi(strings.TrimPrefix(after.VersionID, "v"))
			if err != nil {
				return model.ProjectionPage{}, err
			}
			start = n + 1
		}
		page := model.ProjectionPage{Documents: []model.ProjectionDocument{}}
		for n := start; n < min(start+100, count); n++ {
			page.Documents = append(page.Documents, document(n))
		}
		if start+len(page.Documents) < count {
			next := model.ProjectionDocumentCursor(page.Documents[len(page.Documents)-1])
			page.Next = &next
		}
		return page, nil
	}
	check, err := index.VerifyProjectionPages(ctx, key, 1, loader)
	if err != nil || check.Count != count || check.Version != model.ProjectionInventoryV2 {
		t.Fatal("large inventory", check, err)
	}
	expected := model.NewProjectionInventory()
	for n := 0; n < count; n++ {
		if err = expected.Add(document(n)); err != nil {
			t.Fatal(err)
		}
	}
	if check != expected.Check() {
		t.Fatal("inventory digest differs", check, expected.Check())
	}
	// Retention can leave an empty initial page or a disposed tail after the
	// last retained document. Scan progress does not change the content proof.
	withDisposed := func(ctx context.Context, after model.ProjectionCursor) (model.ProjectionPage, error) {
		if after == (model.ProjectionCursor{}) {
			next := model.ProjectionCursor{Kind: "fact", VersionID: "tombstone"}
			return model.ProjectionPage{Next: &next}, nil
		}
		if after.Kind == "memory" {
			return model.ProjectionPage{}, nil
		}
		if after.VersionID == "tombstone" {
			after = model.ProjectionCursor{}
		}
		page, err := loader(ctx, after)
		if err == nil && page.Next == nil {
			next := model.ProjectionCursor{Kind: "memory", VersionID: "disposed"}
			page.Next = &next
		}
		return page, err
	}
	disposedCheck, err := index.VerifyProjectionPages(ctx, key, 1, withDisposed)
	if err != nil || disposedCheck != check {
		t.Fatal("disposed scan changed inventory", disposedCheck, err)
	}
	// A duplicate page must not substitute for the next canonical page.
	if _, err = index.VerifyProjectionPages(ctx, key, 1, func(ctx context.Context, _ model.ProjectionCursor) (model.ProjectionPage, error) {
		return loader(ctx, model.ProjectionCursor{})
	}); !errors.Is(err, model.ErrInvalid) {
		t.Fatal("repeated page", err)
	}
	// The gate remains held while the source is paged; an unseal cannot pass it.
	if _, err = index.VerifyProjectionPages(ctx, key, 1, func(context.Context, model.ProjectionCursor) (model.ProjectionPage, error) {
		short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		if err := index.SetProjectionGate(short, key, 2, false); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("verification lost gate lock", err)
		}
		return model.ProjectionPage{}, model.ErrUnavailable
	}); !errors.Is(err, model.ErrUnavailable) {
		t.Fatal("loader failure", err)
	}
	// A timed-out write has an ambiguous outcome after the lock is released.
	// A higher seal fence dominates even a delayed opening attempt.
	if err = index.SetProjectionGate(ctx, key, 3, true); err != nil {
		t.Fatal(err)
	}
	lastID := model.ProjectionID(document(count - 1))
	if _, err = admin.ExecContext(ctx, `UPDATE embeddings SET content='tampered' WHERE id=$1`, lastID); err != nil {
		t.Fatal(err)
	}
	if _, err = index.VerifyProjectionPages(ctx, key, 3, loader); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("late-page corruption", err)
	}
	if _, err = admin.ExecContext(ctx, `UPDATE embeddings SET content=$2 WHERE id=$1`, lastID, content); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.ExecContext(ctx, `INSERT INTO embeddings(id,collection_id,vector,content,metadata) SELECT 'extra',collection_id,vector,content,metadata FROM embeddings LIMIT 1`); err != nil {
		t.Fatal(err)
	}
	if _, err = index.VerifyProjectionPages(ctx, key, 3, loader); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("extra physical row", err)
	}
}
