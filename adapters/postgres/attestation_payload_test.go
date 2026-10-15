package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedAttestationPayloadMigration(t *testing.T, ctx context.Context, admin *pgxpool.Pool) map[string][]byte {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	if _, err := admin.Exec(ctx, "INSERT INTO evidence(tenant_id,id,scope_id,source_identity,digest,payload) VALUES('legacy','attestation-evidence','scope','attestation-evidence','digest',decode('01','hex'))"); err != nil {
		t.Fatal(err)
	}
	out := map[string][]byte{}
	for _, kind := range []string{"fact", "memory", "opaque"} {
		table, identityKind, record, version := "attestations", "fact", "record", "payload-fact"
		if kind == "memory" {
			table = "memory_attestations"
			identityKind = "memory"
			record = "payload-memory-record"
			version = "payload-memory"
		}
		if kind == "opaque" {
			version = "a"
		}
		a := model.Attestation{ID: "payload-" + kind + "-attestation", Scope: "scope", RecordID: record, VersionID: version, ContentDigest: "digest", Kind: "bank_verified", EvidenceID: "attestation-evidence", Attestor: model.Caller{Tenant: "legacy", Principal: "reviewer"}}
		raw, err := json.Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := cipher.Seal("legacy", raw)
		if err != nil {
			t.Fatal(err)
		}
		if kind == "opaque" {
			payload = []byte{1, 2, 3}
		}
		if _, err = admin.Exec(ctx, "INSERT INTO attestation_identities(tenant_id,id,scope_id,kind) VALUES('legacy',$1,'scope',$2)", a.ID, identityKind); err != nil {
			t.Fatal(err)
		}
		if _, err = admin.Exec(ctx, "INSERT INTO "+table+"(tenant_id,id,scope_id,record_id,version_id,evidence_id,ledger_revision,payload) VALUES('legacy',$1,'scope',$2,$3,'attestation-evidence',3,$4)", a.ID, record, version, payload); err != nil {
			t.Fatal(err)
		}
		out[kind] = payload
	}
	return out
}

func testAttestationPayloadMigration(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool, want map[string][]byte) {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"fact", "memory"} {
		t.Run(kind+" attestation payload", func(t *testing.T) {
			child := kind + "_attestation_payloads"
			parent, version, archiveQuery, auditQuery := "attestations", "payload-fact", ledgerdb.ListArchiveAttestation, ledgerdb.ListAuditAttestation
			if kind == "memory" {
				parent = "memory_attestations"
				version = "payload-memory"
				archiveQuery = ledgerdb.ListArchiveMemoryAttestation
				auditQuery = ledgerdb.ListAuditMemoryAttestation
			}
			id := "payload-" + kind + "-attestation"
			var payload []byte
			if err := admin.QueryRow(ctx, "SELECT payload FROM "+child+" WHERE tenant_id='legacy' AND attestation_id=$1", id).Scan(&payload); err != nil || !bytes.Equal(payload, want[kind]) {
				t.Fatal("migration changed ciphertext", err)
			}
			var count int
			if err := admin.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND column_name='payload'", parent).Scan(&count); err != nil || count != 0 {
				t.Fatal("header retains content", count, err)
			}
			read := func(historical bool) error {
				return ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
					var attestations []model.Attestation
					var err error
					if kind == "memory" {
						revision := int64(0)
						if historical {
							revision = 3
						}
						attestations, err = tx.(model.MemoryLifecycleTx).MemoryAttestations(ctx, version, revision)
					} else if historical {
						attestations, err = tx.(model.TemporalTx).HistoricalAttestations(ctx, version, 3)
					} else {
						attestations, err = tx.Attestations(ctx, version)
					}
					if err == nil && (len(attestations) != 1 || attestations[0].ID != id) {
						t.Fatal("attestation lost", attestations)
					}
					return err
				})
			}
			for _, historical := range []bool{false, true} {
				if err := read(historical); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := runtime.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if err = ledgerdb.New(tx).BindTenant(ctx, "other"); err != nil {
				t.Fatal(err)
			}
			if err = tx.QueryRow(ctx, "SELECT count(*) FROM "+child+" WHERE tenant_id='legacy'").Scan(&count); err != nil || count != 0 {
				t.Fatal("attestation tenant leak", count, err)
			}
			if err = tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{"DELETE FROM " + child, "UPDATE " + child + " SET payload=decode('01','hex')", "TRUNCATE " + child} {
				_, err := runtime.Exec(ctx, query)
				var pgerr *pgconn.PgError
				if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
					t.Fatal("runtime can mutate attestation", err)
				}
			}
			if _, err = admin.Exec(ctx, "DELETE FROM "+child+" WHERE tenant_id='legacy' AND attestation_id=$1", id); err != nil {
				t.Fatal(err)
			}
			for _, historical := range []bool{false, true} {
				if err := read(historical); !errors.Is(err, model.ErrPrecondition) {
					t.Fatal("missing attestation silently omitted", historical, err)
				}
			}
			err = ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
				_, err := tx.WithdrawAttestation(ctx, id, "scope", model.Caller{Tenant: "legacy", Principal: "reviewer"})
				return err
			})
			if !errors.Is(err, model.ErrPrecondition) {
				t.Fatal("withdrawal accepted missing attestation", err)
			}
			for _, query := range []struct {
				sql string
				arg any
			}{{archiveQuery, int64(3)}, {auditQuery, "scope"}} {
				rows, err := admin.Query(ctx, query.sql, "legacy", query.arg)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for rows.Next() {
					var rowID string
					var content []byte
					if err = rows.Scan(&rowID, &content); err != nil {
						rows.Close()
						t.Fatal(err)
					}
					if rowID == id {
						found = true
						if content != nil {
							rows.Close()
							t.Fatal("unexpected bytes")
						}
					}
				}
				err = rows.Err()
				rows.Close()
				if err != nil || !found {
					t.Fatal("inventory hid missing attestation", err)
				}
			}
			if _, err = admin.Exec(ctx, fmt.Sprintf("INSERT INTO %s(tenant_id,attestation_id,payload) VALUES('legacy',$1,$2)", child), id, want[kind]); err != nil {
				t.Fatal(err)
			}
			if err = read(true); err != nil {
				t.Fatal(err)
			}
		})
	}
	var opaque []byte
	if err = admin.QueryRow(ctx, "SELECT payload FROM fact_attestation_payloads WHERE tenant_id='legacy' AND attestation_id='payload-opaque-attestation'").Scan(&opaque); err != nil || !bytes.Equal(opaque, want["opaque"]) {
		t.Fatal("opaque ciphertext changed", err)
	}
}
