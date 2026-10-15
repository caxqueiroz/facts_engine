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

func testConversationDisposal(t *testing.T, ctx context.Context, admin *pgxpool.Pool, normal model.Ledger, store *evidence.Postgres) {
	t.Helper()
	const tenant, scope = "conversation-retention-bank", "conversation-retention-case"
	caller := model.Caller{Tenant: tenant, Principal: "retention-operator"}
	message, err := store.Put(ctx, tenant, "conversation-disposal-source", []byte("private conversation text"))
	if err != nil {
		t.Fatal(err)
	}
	ingester, _ := facts.New(normal, allow{}, store)
	record, err := ingester.IngestConversation(ctx, caller, model.ConversationRequest{Command: model.Command{Key: "conversation-disposal-ingest", Purpose: "retention-test"}, Scope: scope, SourceSystem: "test-chat", ConversationID: "conversation-disposal", Revision: "v1", Timezone: "UTC", Messages: []model.ConversationMessage{{ID: "message", Revision: "v1", Speaker: "customer", SpeakerRole: "customer", SourceClass: "customer", SourceTime: time.Now().UTC(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "source", Object: message}}})
	if err != nil {
		t.Fatal(err)
	}

	cfg := admin.Config().Copy()
	cfg.ConnConfig.RuntimeParams["role"] = "facts_retention"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{4}, 32))
	retentionLedger, err := postgres.New(ctx, pool, cipher)
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := facts.New(retentionLedger, allow{}, store)
	policy := model.RetentionPolicy{ID: "conversation-retention", Scope: scope, RecordClass: "conversation_payload", Purpose: "retention-test", Jurisdiction: "SG"}
	if _, err = engine.RegisterRetentionPolicy(ctx, caller, model.RetentionPolicyRequest{Command: model.Command{Key: "conversation-retention-policy", Purpose: policy.Purpose}, Policy: policy}); err != nil {
		t.Fatal(err)
	}
	selection := model.ConversationDisposalRequest{Scope: scope, ConversationID: record.ID, PolicyID: policy.ID, Jurisdiction: policy.Jurisdiction, Purpose: policy.Purpose}
	batch := model.DisposalBatchRequest{Command: model.Command{Key: "conversation-disposal-batch", Purpose: policy.Purpose}, Scope: scope, Reason: "approved retention schedule", Conversations: []model.ConversationDisposalRequest{selection}}
	result, err := engine.DisposeBatch(ctx, caller, batch)
	if err != nil || result.ID == "" {
		t.Fatal("conversation batch disposal", result, err)
	}
	testDisposalBatchRemote(t, ctx, engine, caller, batch, result)
	var payloadCount int
	if err = admin.QueryRow(ctx, "SELECT count(*) FROM conversation_payloads WHERE tenant_id=$1 AND conversation_id=$2", tenant, record.ID).Scan(&payloadCount); err != nil || payloadCount != 0 {
		t.Fatal("conversation payload remains", payloadCount, err)
	}
	var encrypted []byte
	if err = admin.QueryRow(ctx, "SELECT payload FROM conversation_disposals WHERE tenant_id=$1 AND conversation_id=$2", tenant, record.ID).Scan(&encrypted); err != nil {
		t.Fatal(err)
	}
	plain, err := cipher.Open(tenant, encrypted)
	if err != nil {
		t.Fatal(err)
	}
	var tombstone model.ConversationRecord
	if err = json.Unmarshal(plain, &tombstone); err != nil || !model.ValidConversationTombstone(tombstone, tenant, record.ID) || len(tombstone.EvidenceIDs) != 1 || len(tombstone.Request.Messages) != 0 {
		t.Fatal("invalid conversation tombstone", tombstone, err)
	}
	if _, err = ingester.GetConversation(ctx, caller, model.ConversationQuery{ID: record.ID, Scope: scope, Purpose: policy.Purpose}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("disposed conversation was readable", err)
	}
	// Exercise the standalone RPC as well as DisposeBatch. They share the same
	// tombstone implementation, but each public transport must preserve the
	// command receipt and retry semantics independently.
	secondMessage, err := store.Put(ctx, tenant, "conversation-disposal-source-2", []byte("second private conversation text"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := ingester.IngestConversation(ctx, caller, model.ConversationRequest{Command: model.Command{Key: "conversation-disposal-ingest-2", Purpose: "retention-test"}, Scope: scope, SourceSystem: "test-chat", ConversationID: "conversation-disposal-2", Revision: "v1", Timezone: "UTC", Messages: []model.ConversationMessage{{ID: "message-2", Revision: "v1", Speaker: "customer", SpeakerRole: "customer", SourceClass: "customer", SourceTime: time.Now().UTC(), MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "source", Object: secondMessage}}})
	if err != nil {
		t.Fatal("ingest second conversation", err)
	}
	standalone := model.ConversationDisposalRequest{Command: model.Command{Key: "conversation-disposal-standalone", Purpose: policy.Purpose}, Scope: scope, ConversationID: second.ID, PolicyID: policy.ID, Jurisdiction: policy.Jurisdiction, Purpose: policy.Purpose, Reason: "approved retention schedule"}
	testRemoteConversationDisposal(t, ctx, engine, caller, standalone)
	if _, err := ingester.GetConversation(ctx, caller, model.ConversationQuery{ID: second.ID, Scope: scope, Purpose: policy.Purpose}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("standalone disposed conversation was readable", err)
	}
	var replay model.CommitResult
	replay, err = engine.DisposeBatch(ctx, caller, model.DisposalBatchRequest{Command: model.Command{Key: "conversation-disposal-batch", Purpose: policy.Purpose}, Scope: scope, Reason: "approved retention schedule", Conversations: []model.ConversationDisposalRequest{selection}})
	if err != nil || replay.ID != result.ID {
		t.Fatal("conversation batch replay", replay, err)
	}
}
