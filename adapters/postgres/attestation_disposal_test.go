package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testAttestationDisposal(t *testing.T, ctx context.Context, admin *pgxpool.Pool, store *evidence.Postgres, payloads map[string][]byte) {
	cfg := admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["role"] = "facts_retention"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := facts.New(ledger, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	caller := model.Caller{Tenant: "legacy", Principal: "retention-operator"}
	for _, kind := range []string{"fact", "memory"} {
		t.Run(kind, func(t *testing.T) {
			id := "payload-" + kind + "-attestation"
			payloadTable := kind + "_attestation_payloads"
			var original []byte
			if err = admin.QueryRow(ctx, "SELECT payload FROM "+payloadTable+" WHERE tenant_id='legacy' AND attestation_id=$1", id).Scan(&original); err != nil || !bytes.Equal(original, payloads[kind]) {
				t.Fatal("unexpected fixture bytes", err)
			}
			record := "record"
			if kind == "memory" {
				record = "payload-memory-record"
			}
			policy := model.RetentionPolicy{ID: "attestation-retention-" + kind, Scope: "scope", RecordClass: "attestation_payload", Purpose: "retention-test", Jurisdiction: "SG"}
			if _, err := engine.RegisterRetentionPolicy(ctx, caller, model.RetentionPolicyRequest{Command: model.Command{Key: "policy-" + kind, Purpose: policy.Purpose}, Policy: policy}); err != nil {
				t.Fatal(err)
			}
			version := "payload-fact"
			if kind == "memory" {
				version = "payload-memory"
			}
			r := model.AttestationDisposalRequest{Command: model.Command{Key: "dispose-" + kind, Purpose: policy.Purpose}, Kind: kind, Scope: "scope", RecordID: record, VersionID: version, AttestationID: id, PolicyID: policy.ID, Jurisdiction: policy.Jurisdiction, Purpose: policy.Purpose, Reason: "approved schedule"}
			young := policy
			young.ID += "-young"
			young.MinimumAgeSeconds = 3600
			if _, err = engine.RegisterRetentionPolicy(ctx, caller, model.RetentionPolicyRequest{Command: model.Command{Key: "policy-young-" + kind, Purpose: policy.Purpose}, Policy: young}); err != nil {
				t.Fatal(err)
			}
			tooYoung := r
			tooYoung.Key = "dispose-young-" + kind
			tooYoung.PolicyID = young.ID
			if _, err = engine.DisposeAttestation(ctx, caller, tooYoung); !errors.Is(err, model.ErrPrecondition) {
				t.Fatal("minimum age was bypassed", err)
			}
			holdID := "hold-attestation-" + kind
			if _, err = engine.PlaceLegalHold(ctx, caller, model.LegalHoldRequest{Command: model.Command{Key: "place-" + holdID, Purpose: policy.Purpose}, ID: holdID, Scope: policy.Scope, Reason: "investigation"}); err != nil {
				t.Fatal(err)
			}
			held := r
			held.Key = "dispose-held-" + kind
			if _, err = engine.DisposeAttestation(ctx, caller, held); !errors.Is(err, model.ErrPrecondition) {
				t.Fatal("active legal hold was bypassed", err)
			}
			if _, err = engine.ReleaseLegalHold(ctx, caller, model.ReleaseLegalHoldRequest{Command: model.Command{Key: "release-" + holdID, Purpose: policy.Purpose}, ID: holdID, Scope: policy.Scope, Reason: "investigation closed"}); err != nil {
				t.Fatal(err)
			}
			if kind == "fact" {
				if _, err = admin.Exec(ctx, "REVOKE DELETE ON fact_attestation_payloads FROM facts_retention"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _, _ = admin.Exec(ctx, "GRANT DELETE ON fact_attestation_payloads TO facts_retention") })
				denied, denyErr := engine.DisposeAttestation(ctx, caller, r)
				if !errors.Is(denyErr, model.ErrDenied) {
					var exports, lagging bool
					_ = admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM audit_export_requests WHERE tenant_id='legacy' AND scope_id='scope'), EXISTS(SELECT 1 FROM consumer_offsets WHERE tenant_id='legacy' AND consumer_id LIKE 'archive-%' AND acknowledged_revision<3)`).Scan(&exports, &lagging)
					t.Fatal("retention role bypassed database delete grant", denied, denyErr, "exports", exports, "archive lag", lagging)
				}
				if _, err = admin.Exec(ctx, "GRANT DELETE ON fact_attestation_payloads TO facts_retention"); err != nil {
					t.Fatal(err)
				}
			}
			result, err := engine.DisposeAttestation(ctx, caller, r)
			if err != nil || result.RecordID != record || result.StreamRevision < 1 {
				t.Fatal(result, err)
			}
			table := payloadTable
			markerTable := kind + "_attestation_disposals"
			var remaining int
			if err = admin.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE tenant_id='legacy' AND attestation_id=$1", id).Scan(&remaining); err != nil || remaining != 0 {
				t.Fatal("attestation bytes remain", remaining, err)
			}
			var marker []byte
			if err = admin.QueryRow(ctx, "SELECT payload FROM "+markerTable+" WHERE tenant_id='legacy' AND attestation_id=$1", id).Scan(&marker); err != nil {
				t.Fatal(err)
			}
			plain, err := cipher.Open("legacy", marker)
			if err != nil {
				t.Fatal(err)
			}
			var tombstone model.Attestation
			if err = json.Unmarshal(plain, &tombstone); err != nil || !model.ValidAttestationTombstone(tombstone, "legacy", id) {
				t.Fatal("invalid retained tombstone", err)
			}
			if tombstone.ReviewAuthorization != nil || tombstone.EvidenceAuthorization != nil || tombstone.Delegation != nil || (tombstone.Confirmation != nil && (tombstone.Confirmation.PresentationVersion != "" || !tombstone.Confirmation.PresentedAt.IsZero() || !tombstone.Confirmation.RespondedAt.IsZero())) {
				t.Fatal("private attestation content remains in tombstone")
			}
			testAttestationDisposalRemote(t, ctx, engine, caller, r, result)
			for _, historical := range []bool{false, true} {
				err = ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
					var out []model.Attestation
					var readErr error
					if kind == "memory" {
						revision := int64(0)
						if historical {
							revision = 3
						}
						out, readErr = tx.(model.MemoryLifecycleTx).MemoryAttestations(ctx, "payload-memory", revision)
					} else if historical {
						out, readErr = tx.(model.TemporalTx).HistoricalAttestations(ctx, "payload-fact", 3)
					} else {
						out, readErr = tx.Attestations(ctx, "payload-fact")
					}
					if readErr == nil && (len(out) != 1 || out[0].ID != id || !out[0].PayloadDisposed || !model.ValidAttestationTombstone(out[0], "legacy", id)) {
						t.Fatal("disposed attestation tombstone missing from lifecycle inventory", out)
					}
					return readErr
				})
				if err != nil {
					t.Fatal("disposed attestation broke lifecycle inventory", historical, err)
				}
			}
		})
	}
}
