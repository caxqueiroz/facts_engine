package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testOrphanDisposal(t *testing.T, ctx context.Context, normal, engine *facts.Engine, ledger *postgres.Ledger, store *evidence.Postgres, admin *pgxpool.Pool, c model.Caller) {
	t.Helper()
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "cleanup"} }
	policy := model.RetentionPolicy{ID: "orphan-immediate", Scope: "case", RecordClass: "orphan_evidence_bytes", Purpose: "cleanup", Jurisdiction: "XX"}
	if _, err := engine.RegisterRetentionPolicy(ctx, c, model.RetentionPolicyRequest{Command: cmd("orphan-immediate"), Policy: policy}); err != nil {
		t.Fatal(err)
	}
	body := []byte("unregistered upload")
	upload := model.PendingEvidenceUpload{ID: "orphan-delete", Scope: "case", PolicyID: policy.ID}
	ref, err := store.PutPending(ctx, c.Tenant, upload, body)
	if err != nil {
		t.Fatal(err)
	}
	request := model.OrphanEvidenceDisposalRequest{Command: cmd("orphan-delete"), Scope: "case", PolicyID: policy.ID, Jurisdiction: "XX", Reason: "expired upload", Object: ref}
	reject := func(e *facts.Engine, r model.OrphanEvidenceDisposalRequest, want error) {
		t.Helper()
		if _, err := e.DisposeOrphanEvidence(ctx, c, r); !errors.Is(err, want) {
			t.Fatalf("orphan rejection for scope %s object %s: got %v want %v", r.Scope, r.Object.Locator, err, want)
		}
	}
	reject(normal, request, model.ErrDenied)
	denied, _ := facts.New(ledger, denyPermission{permission: "can_dispose"}, store)
	reject(denied, request, model.ErrDenied)
	for _, change := range []func(*model.OrphanEvidenceDisposalRequest){func(r *model.OrphanEvidenceDisposalRequest) { r.PolicyID = "orphan-policy" }, func(r *model.OrphanEvidenceDisposalRequest) { r.Jurisdiction = "US" }, func(r *model.OrphanEvidenceDisposalRequest) { r.Purpose = "other" }, func(r *model.OrphanEvidenceDisposalRequest) { r.Object.Size++ }} {
		r := request
		change(&r)
		reject(engine, r, model.ErrPrecondition)
	}
	hold := model.LegalHoldRequest{Command: cmd("orphan-hold"), ID: "orphan-hold", Scope: "case", Reason: "review"}
	if _, err := engine.PlaceLegalHold(ctx, c, hold); err != nil {
		t.Fatal(err)
	}
	reject(engine, request, model.ErrPrecondition)
	if _, err := engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: cmd("orphan-release"), ID: hold.ID, Scope: hold.Scope, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	failing, _ := facts.New(rollbackRetentionLedger{ledger}, allow{}, store)
	reject(failing, request, model.ErrUnavailable)
	if _, err := store.Read(ctx, c.Tenant, ref); err != nil {
		t.Fatal("rollback lost bytes", err)
	}
	young, err := store.PutPending(ctx, c.Tenant, model.PendingEvidenceUpload{ID: "orphan-young", Scope: "case", PolicyID: "orphan-policy"}, body)
	if err != nil {
		t.Fatal(err)
	}
	r := request
	r.Object = young
	r.PolicyID = "orphan-policy"
	reject(engine, r, model.ErrPrecondition)
	// Registered pending uploads and untracked objects must never be cleaned up.

	registered, err := store.PutPending(ctx, c.Tenant, model.PendingEvidenceUpload{ID: "orphan-registered", Scope: "case", PolicyID: policy.ID}, body)
	if err != nil {
		t.Fatal(err)
	}
	register := func(ref model.ObjectRef, id string) error {
		_, err := normal.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: cmd(id), Evidence: model.Evidence{ID: id, Scope: "case", SourceSystem: "test", SourceObject: id, SourceRevision: "v1", Speaker: "staff", SourceClass: "organization_record", SourceTime: time.Now().UTC(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "short-lived", Object: ref}})
		return err
	}
	if err := register(registered, "orphan-registered"); err != nil {
		t.Fatal(err)
	}
	r = request
	r.Object = registered
	reject(engine, r, model.ErrPrecondition)
	if _, err := store.Read(ctx, c.Tenant, registered); err != nil {
		t.Fatal("registered bytes lost", err)
	}
	// Simulate a pre-inventory record in this disposable fixture. Even an
	// unrelated unbound record must block cleanup because its locator is encrypted.
	var registeredAt time.Time
	if err := admin.QueryRow(ctx, "DELETE FROM evidence_retention WHERE tenant_id=$1 AND evidence_id=$2 RETURNING registered_at", c.Tenant, "orphan-registered").Scan(&registeredAt); err != nil {
		t.Fatal(err)
	}
	reject(engine, request, model.ErrPrecondition)
	if _, err := admin.Exec(ctx, "INSERT INTO evidence_retention(tenant_id,evidence_id,scope_id,locator,object_version,registered_at) VALUES($1,$2,$3,$4,$5,$6)", c.Tenant, "orphan-registered", "case", registered.Locator, registered.Version, registeredAt); err != nil {
		t.Fatal(err)
	}
	r = request
	r.Scope = "other-scope"
	reject(engine, r, model.ErrDenied)
	// Registration and deletion race on the same object epoch. Exactly one may
	// succeed; if registration wins, its bytes must remain readable.
	raced, err := store.PutPending(ctx, c.Tenant, model.PendingEvidenceUpload{ID: "orphan-raced", Scope: "case", PolicyID: policy.ID}, body)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	registeredResult := make(chan error, 1)
	go func() { <-start; registeredResult <- register(raced, "orphan-raced") }()
	r = request
	r.Object = raced
	r.Key = "orphan-raced-dispose"
	close(start)
	_, disposeErr := engine.DisposeOrphanEvidence(ctx, c, r)
	registerErr := <-registeredResult
	if (disposeErr == nil) == (registerErr == nil) {
		t.Fatal("race must have one winner", disposeErr, registerErr)
	}
	loser := disposeErr
	if loser == nil {
		loser = registerErr
	}
	if !errors.Is(loser, model.ErrPrecondition) {
		t.Fatal("unexpected race error", loser)
	}
	if registerErr == nil {
		if _, err := store.Read(ctx, c.Tenant, raced); err != nil {
			t.Fatal("race deleted registered evidence", err)
		}
	}
	raw, err := store.Put(ctx, c.Tenant, "orphan-untracked", body)
	if err != nil {
		t.Fatal(err)
	}
	r = request
	r.Object = raw
	reject(engine, r, model.ErrPrecondition)
	result, err := engine.DisposeOrphanEvidence(ctx, c, request)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := engine.DisposeOrphanEvidence(ctx, c, request)
	if err != nil || retry != result {
		t.Fatal("orphan replay", retry, err)
	}
	r = request
	r.Reason = "changed"
	reject(engine, r, model.ErrConflict)
	if _, err := store.Read(ctx, c.Tenant, ref); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("orphan bytes retained", err)
	}
	if _, err := store.PutPending(ctx, c.Tenant, upload, body); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("orphan resurrected", err)
	}
	if _, err := store.Put(ctx, c.Tenant, upload.ID, body); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("raw orphan resurrected", err)
	}
	var encrypted []byte
	if err := admin.QueryRow(ctx, "SELECT payload FROM retention_events WHERE tenant_id=$1 AND id=$2 AND kind='dispose_orphan_evidence'", c.Tenant, result.ID).Scan(&encrypted); err != nil {
		t.Fatal(err)
	}
	// Decode using the fixture key to verify the durable event binds the exact version.
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	plain, err := cipher.Open(c.Tenant, encrypted)
	if err != nil {
		t.Fatal(err)
	}
	var event model.RetentionEvent
	if err := json.Unmarshal(plain, &event); err != nil || event.OrphanDisposal == nil || *event.OrphanDisposal != request || event.LedgerRevision != result.LedgerRevision {
		t.Fatal("orphan audit binding", event, err)
	}
	var retained int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM pending_evidence_uploads WHERE tenant_id=$1 AND locator=$2", c.Tenant, ref.Locator).Scan(&retained); err != nil || retained != 1 {
		t.Fatal("upload provenance removed", retained, err)
	}
}
