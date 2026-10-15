package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type nullUploadCipher struct{ postgres.Cipher }

func (nullUploadCipher) Seal(string, []byte) ([]byte, error) { return nil, nil }

func testPendingUploads(t *testing.T, ctx context.Context, normal, retention *facts.Engine, store *evidence.Postgres, pool, admin *pgxpool.Pool, cipher postgres.Cipher, caller model.Caller) {
	t.Helper()
	client := v1.NewRetentionServiceClient(governanceGRPC(t, retention, caller))
	policy := &v1.RegisterRetentionPolicyRequest{Tenant: caller.Tenant, Command: &v1.Command{Key: "orphan-policy", Purpose: "cleanup"}, Policy: &v1.RetentionPolicy{Id: "orphan-policy", Scope: "case", RecordClass: "orphan_evidence_bytes", Purpose: "cleanup", Jurisdiction: "SG", MinimumAgeSeconds: 3600}}
	if _, err := client.RegisterRetentionPolicy(ctx, policy); err != nil {
		t.Fatal("orphan policy over gRPC", err)
	}
	upload := model.PendingEvidenceUpload{ID: "pending-object", Scope: "case", PolicyID: "orphan-policy"}
	body := []byte("pending evidence")
	ref, err := store.PutPending(ctx, caller.Tenant, upload, body)
	if err != nil {
		t.Fatal(err)
	}
	readMetadata := func(ref model.ObjectRef) (string, string, int64, time.Time) {
		t.Helper()
		var scope, policy string
		var size int64
		var at time.Time
		err := admin.QueryRow(ctx, "SELECT scope_id,policy_id,byte_size,uploaded_at FROM pending_evidence_uploads WHERE tenant_id=$1 AND locator=$2 AND object_version=$3", caller.Tenant, ref.Locator, ref.Version).Scan(&scope, &policy, &size, &at)
		if err != nil {
			t.Fatal(err)
		}
		return scope, policy, size, at
	}
	scope, policyID, size, firstAt := readMetadata(ref)
	if scope != upload.Scope || policyID != upload.PolicyID || size != int64(len(body)) || firstAt.IsZero() {
		t.Fatal("upload provenance", scope, policyID, size, firstAt)
	}
	replay, err := store.PutPending(ctx, caller.Tenant, upload, body)
	if err != nil || replay != ref {
		t.Fatal("pending retry", replay, err)
	}
	_, _, _, againAt := readMetadata(ref)
	if !againAt.Equal(firstAt) {
		t.Fatal("retry reset upload age")
	}
	for _, changed := range []model.PendingEvidenceUpload{{ID: upload.ID, Scope: "other", PolicyID: upload.PolicyID}, {ID: upload.ID, Scope: upload.Scope, PolicyID: "short-lived"}} {
		if _, err := store.PutPending(ctx, caller.Tenant, changed, body); !errors.Is(err, model.ErrConflict) {
			t.Fatal("upload binding changed", err)
		}
	}
	for _, policy := range []string{"short-lived", "missing-policy"} {
		r := model.PendingEvidenceUpload{ID: "pending-" + policy, Scope: "case", PolicyID: policy}
		ref, err := store.PutPending(ctx, caller.Tenant, r, body)
		if !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("invalid cleanup policy accepted", err)
		}
		if _, err := store.Read(ctx, caller.Tenant, ref); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("failed upload left bytes", err)
		}
	}
	legacy, err := store.Put(ctx, caller.Tenant, "legacy-upload", body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutPending(ctx, caller.Tenant, model.PendingEvidenceUpload{ID: legacy.Locator, Scope: "case", PolicyID: upload.PolicyID}, body); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("legacy upload adopted", err)
	}
	if b, err := store.Read(ctx, caller.Tenant, legacy); err != nil || !bytes.Equal(b, body) {
		t.Fatal("legacy bytes changed", err)
	}
	broken, err := evidence.NewPostgres(pool, nullUploadCipher{cipher})
	if err != nil {
		t.Fatal(err)
	}
	failed := upload
	failed.ID = "pending-rollback"
	if _, err := broken.PutPending(ctx, caller.Tenant, failed, body); err == nil {
		t.Fatal("NULL payload unexpectedly stored")
	}
	var count int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM pending_evidence_uploads WHERE tenant_id=$1 AND locator=$2", caller.Tenant, failed.ID).Scan(&count); err != nil || count != 0 {
		t.Fatal("upload metadata survived payload rollback", count, err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id',$1,true)", "other-tenant"); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM pending_evidence_uploads WHERE tenant_id=$1", caller.Tenant).Scan(&count); err != nil || count != 0 {
		t.Fatal("pending upload tenant leak", count, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"UPDATE pending_evidence_uploads SET uploaded_at=clock_timestamp()", "DELETE FROM pending_evidence_uploads", "INSERT INTO pending_evidence_uploads(tenant_id,locator,object_version,scope_id,policy_id,byte_size,uploaded_at) VALUES('x','x','x','x','x',0,clock_timestamp())"} {
		_, err := pool.Exec(ctx, query)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Fatal("runtime altered upload provenance", err)
		}
	}
	if _, err := normal.RegisterEvidence(ctx, caller, model.EvidenceRequest{Command: model.Command{Key: "pending-register", Purpose: "review"}, Evidence: model.Evidence{ID: "pending-registered", Scope: "case", SourceSystem: "test", SourceObject: "pending", SourceRevision: "v1", Speaker: "staff", SourceClass: "bank_record", SourceTime: time.Now().UTC(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "short-lived", Object: ref}}); err != nil {
		t.Fatal("pending object registration", err)
	}
	_, _, _, registeredAt := readMetadata(ref)
	if !registeredAt.Equal(firstAt) {
		t.Fatal("registration rewrote upload provenance")
	}
}
