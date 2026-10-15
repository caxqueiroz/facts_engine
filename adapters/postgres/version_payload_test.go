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

type legacyVersionPayloads struct {
	fact, memory  []byte
	factVersion   model.Version
	memoryVersion model.MemoryVersion
}

func seedVersionPayloadMigration(t *testing.T, ctx context.Context, admin *pgxpool.Pool) legacyVersionPayloads {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	seal := func(v any) []byte {
		t.Helper()
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		b, err := cipher.Seal("legacy", raw)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	value := "legacy private fact"
	fact := model.Version{ID: "payload-fact", RecordID: "record", PolicyID: "policy", Proposer: model.Caller{Tenant: "legacy", Principal: "proposer"}, Proposal: model.FactProposal{Key: model.FactKey{Scope: "scope", Subject: "subject", Predicate: "predicate"}, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &value}, Sources: []model.SourceSpan{{EvidenceID: "legacy-source", Quote: "legacy source quote"}}}}
	fact.Digest = model.Digest(struct {
		Key     model.FactKey
		Value   model.Value
		Valid   model.Interval
		Schema  string
		Sources []model.SourceSpan
	}{fact.Proposal.Key, fact.Proposal.Value, fact.Proposal.Valid, fact.Proposal.SchemaVersion, fact.Proposal.Sources})
	memory := model.MemoryVersion{ID: "payload-memory", RecordID: "payload-memory-record", PolicyID: "policy", LedgerRevision: 3, Proposer: fact.Proposer, Proposal: model.MemoryProposal{Key: model.MemoryKey{Scope: "scope", ID: "summary"}, Subtype: "episode", SchemaVersion: "v1", Body: "legacy private memory", Sources: fact.Proposal.Sources, Dependencies: []model.MemoryDependency{{Scope: "scope", RecordID: fact.RecordID, VersionID: fact.ID, ContentDigest: fact.Digest, Label: "assertion"}}}}
	memory.Digest = model.MemoryDigest(memory.Proposal)
	f, m := seal(fact), seal(memory)
	if _, err := admin.Exec(ctx, "INSERT INTO record_versions(tenant_id,record_id,id,scope_id,predicate,schema_version,digest,payload) VALUES('legacy','record',$1,'scope','predicate','v1',$2,$3)", fact.ID, fact.Digest, f); err != nil {
		t.Fatal(err)
	}
	event := model.Event{ID: "payload-event", Type: "propose_fact", Scope: "scope", RecordID: fact.RecordID, VersionID: fact.ID, LedgerRevision: 3, StreamRevision: 5, Caller: fact.Proposer}
	if _, err := admin.Exec(ctx, "INSERT INTO ledger_events(tenant_id,id,ledger_revision,event_type,scope_id,payload) VALUES('legacy',$1,3,'propose_fact','scope',$2)", event.ID, seal(event)); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"UPDATE tenant_revisions SET revision=3 WHERE tenant_id='legacy'",
		"UPDATE records SET stream_revision=5 WHERE tenant_id='legacy' AND id='record'",
		"INSERT INTO record_events(tenant_id,event_id,record_id,version_id,stream_revision) VALUES('legacy','payload-event','record','payload-fact',5)",
		"INSERT INTO records(tenant_id,id,scope_id,subject_id,predicate,discriminator,kind) VALUES('legacy','payload-memory-record','scope','','','summary','memory')",
	} {
		if _, err := admin.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.Exec(ctx, "INSERT INTO memory_types(tenant_id,scope_id,subtype,version,policy_id,ledger_revision,payload) VALUES('legacy','scope','episode','v1','policy',3,$1)", seal(model.MemoryType{Scope: "scope", Subtype: "episode", Version: "v1", PolicyID: "policy"})); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO memory_versions(tenant_id,id,record_id,scope_id,subtype,schema_version,ledger_revision,payload) VALUES('legacy',$1,$2,'scope','episode','v1',3,$3)", memory.ID, memory.RecordID, m); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO memory_dependencies(tenant_id,memory_version_id,scope_id,record_id,version_id) VALUES('legacy',$1,'scope','record',$2)", memory.ID, fact.ID); err != nil {
		t.Fatal(err)
	}
	return legacyVersionPayloads{f, m, fact, memory}
}

func testVersionPayloadMigration(t *testing.T, ctx context.Context, admin, runtime *pgxpool.Pool, want legacyVersionPayloads) {
	t.Helper()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	ledger, err := postgres.New(ctx, runtime, cipher)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
		f, err := tx.Version(ctx, want.factVersion.RecordID, want.factVersion.ID)
		if err != nil {
			return err
		}
		if model.Digest(f) != model.Digest(want.factVersion) {
			t.Fatal("fact content changed")
		}
		m, err := tx.(model.MemoryTx).MemoryVersion(ctx, want.memoryVersion.RecordID, want.memoryVersion.ID)
		if err != nil {
			return err
		}
		if model.Digest(m) != model.Digest(want.memoryVersion) {
			t.Fatal("memory content changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		table, parent, id string
		payload           []byte
		query             string
		arg               any
	}{
		{"fact_version_payloads", "record_versions", want.factVersion.ID, want.fact, ledgerdb.ListAuditVersion, "scope"},
		{"memory_version_payloads", "memory_versions", want.memoryVersion.ID, want.memory, ledgerdb.ListArchiveMemoryVersion, int64(3)},
	} {
		t.Run(fixture.table, func(t *testing.T) {
			var b []byte
			if err := admin.QueryRow(ctx, fmt.Sprintf("SELECT payload FROM %s WHERE tenant_id='legacy' AND version_id=$1", fixture.table), fixture.id).Scan(&b); err != nil || !bytes.Equal(b, fixture.payload) {
				t.Fatal("ciphertext changed", err)
			}
			var count int
			if err := admin.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND column_name='payload'", fixture.parent).Scan(&count); err != nil || count != 0 {
				t.Fatal("payload retained in header", count, err)
			}
			tx, err := runtime.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := ledgerdb.New(tx).BindTenant(ctx, "other"); err != nil {
				t.Fatal(err)
			}
			if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE tenant_id='legacy'", fixture.table)).Scan(&count); err != nil || count != 0 {
				t.Fatal("tenant leak", count, err)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{fmt.Sprintf("DELETE FROM %s", fixture.table), fmt.Sprintf("UPDATE %s SET payload=decode('00','hex')", fixture.table), fmt.Sprintf("TRUNCATE %s", fixture.table)} {
				_, err := runtime.Exec(ctx, query)
				var pg *pgconn.PgError
				if !errors.As(err, &pg) || pg.Code != "42501" {
					t.Fatal("runtime changed immutable content", err)
				}
			}
			if _, err := admin.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE tenant_id='legacy' AND version_id=$1", fixture.table), fixture.id); err != nil {
				t.Fatal(err)
			}
			// Both missing bytes and explicitly disposed metadata must fail closed,
			// even if a flagged envelope still contains the original sensitive fields.
			for _, state := range []string{"missing", "tombstone", "flagged-content"} {
				if state != "missing" {
					var content any
					if fixture.table == "fact_version_payloads" {
						v := want.factVersion
						v.PayloadDisposed = true
						if state == "tombstone" {
							v = model.VersionTombstone(v)
						}
						content = v
					} else {
						v := want.memoryVersion
						v.PayloadDisposed = true
						if state == "tombstone" {
							v = model.MemoryVersionTombstone(v)
						}
						content = v
					}
					raw, err := json.Marshal(content)
					if err != nil {
						t.Fatal(err)
					}
					sealed, err := cipher.Seal("legacy", raw)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := admin.Exec(ctx, fmt.Sprintf("INSERT INTO %s(tenant_id,version_id,payload) VALUES('legacy',$1,$2) ON CONFLICT(tenant_id,version_id) DO UPDATE SET payload=EXCLUDED.payload", fixture.table), fixture.id, sealed); err != nil {
						t.Fatal(err)
					}
				}
				for _, history := range []bool{false, true} {
					err := ledger.Transact(ctx, "legacy", func(tx model.Tx) error {
						if fixture.table == "fact_version_payloads" {
							if history {
								_, err := tx.(model.TemporalTx).Versions(ctx, want.factVersion.RecordID, 3)
								return err
							}
							_, err := tx.Version(ctx, want.factVersion.RecordID, fixture.id)
							return err
						}
						if history {
							_, err := tx.(interface {
								MemoryVersions(context.Context, string, int64) ([]model.MemoryVersion, error)
							}).MemoryVersions(ctx, want.memoryVersion.RecordID, 3)
							return err
						}
						_, err := tx.(model.MemoryTx).MemoryVersion(ctx, want.memoryVersion.RecordID, fixture.id)
						return err
					})
					if !errors.Is(err, model.ErrPrecondition) {
						t.Fatal("unavailable content not reported", state, history, err)
					}
				}
			}
			if _, err := admin.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE tenant_id='legacy' AND version_id=$1", fixture.table), fixture.id); err != nil {
				t.Fatal(err)
			}
			rows, err := admin.Query(ctx, fixture.query, "legacy", fixture.arg)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for rows.Next() {
				var id string
				var payload []byte
				if err := rows.Scan(&id, &payload); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				if id == fixture.id {
					found = true
					if payload != nil {
						rows.Close()
						t.Fatal("unexpected content")
					}
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil || !found {
				t.Fatal("audit/archive hid missing version", err)
			}
			if _, err := admin.Exec(ctx, fmt.Sprintf("INSERT INTO %s(tenant_id,version_id,payload) VALUES('legacy',$1,$2)", fixture.table), fixture.id, fixture.payload); err != nil {
				t.Fatal(err)
			}
		})
	}
	// The migration also preserves opaque pre-existing ciphertext it cannot decode.
	var opaque []byte
	if err := admin.QueryRow(ctx, "SELECT payload FROM fact_version_payloads WHERE tenant_id='legacy' AND version_id='a'").Scan(&opaque); err != nil || !bytes.Equal(opaque, []byte{1}) {
		t.Fatal("opaque legacy bytes changed", err)
	}
}
