package postgres_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type contextDisposalIndex struct {
	model.RetrievalIndex
	hits map[string][]model.SearchCandidate
}

func (i *contextDisposalIndex) Candidates(_ context.Context, _, _, scope, _ string, _ int) ([]model.SearchCandidate, error) {
	return i.hits[scope], nil
}
func (*contextDisposalIndex) DescribeRetrieval(context.Context, string, string) (model.RetrievalIdentity, error) {
	return model.RetrievalIdentity{Generation: "context-fixture", EmbeddingID: "none", SearchVersion: "fixture", RerankerID: "none"}, nil
}

func testContextDisposal(t *testing.T, ctx context.Context, normal *facts.Engine, store *evidence.Postgres, admin *pgxpool.Pool) {
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
	_, private, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := audit.NewEd25519("context-key", private)
	engine, err := facts.NewWithAudit(ledger, allow{}, store, model.AuditOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	c := model.Caller{Tenant: "context-retention", Principal: "proposer"}
	reviewer := c
	reviewer.Principal = "reviewer"
	command := func(key string) model.Command { return model.Command{Key: key, Purpose: "context-retention"} }
	now := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	secret := "context copied value"
	index := &contextDisposalIndex{hits: map[string][]model.SearchCandidate{}}
	var dependency model.MemoryDependency
	for _, scope := range []string{"case1", "case2"} {
		registerTestIdentities(t, ctx, normal, c.Tenant, scope, []string{"subject"})
		if _, err := normal.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: command(scope + "-acceptance"), Policy: model.AcceptancePolicy{Scope: scope, ID: "manual", Mode: "manual", SourceClasses: []string{"customer"}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := normal.RegisterPredicateVersion(ctx, c, model.PredicateRequest{Command: command(scope + "-predicate"), Definition: model.PredicateVersion{Scope: scope, Predicate: "value", Version: "v1", PolicyID: "manual", Kind: model.Text, Classification: "confidential"}}); err != nil {
			t.Fatal(err)
		}
		ref, err := store.Put(ctx, c.Tenant, scope+"-source", []byte(secret))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := normal.RegisterEvidence(ctx, c, model.EvidenceRequest{Command: command(scope + "-evidence"), Evidence: model.Evidence{ID: scope + "-source", Scope: scope, SourceSystem: "test", SourceObject: scope, SourceRevision: "v1", Speaker: "customer", SourceClass: "customer", SourceTime: now, MediaType: "text/plain", Classification: "confidential", RetentionPolicy: "source-policy", Object: ref}}); err != nil {
			t.Fatal(err)
		}
		key := model.FactKey{Scope: scope, Subject: "subject", Predicate: "value"}
		proposed, err := normal.ProposeFact(ctx, c, model.FactProposal{Command: command(scope + "-propose"), Key: key, SchemaVersion: "v1", Value: model.Value{Kind: model.Text, Text: &secret}, Valid: model.Interval{From: &now}, Sources: []model.SourceSpan{{EvidenceID: scope + "-source", Start: 0, End: len(secret), Quote: secret}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := normal.ApproveFact(ctx, reviewer, model.Approval{Command: command(scope + "-approve"), Scope: scope, RecordID: proposed.RecordID, VersionID: proposed.VersionID, ExpectedRevision: proposed.StreamRevision}); err != nil {
			t.Fatal(err)
		}
		index.hits[scope] = []model.SearchCandidate{{RecordID: proposed.RecordID, VersionID: proposed.VersionID, Score: 1}}
		if scope == "case2" {
			dependency = model.MemoryDependency{Scope: scope, RecordID: proposed.RecordID, VersionID: proposed.VersionID, ContentDigest: proposed.ContentDigest, Label: "assertion"}
		}
		for _, policy := range []model.RetentionPolicy{{ID: "contexts", Scope: scope, RecordClass: "context_payload", Purpose: "context-retention", Jurisdiction: "SG"}, {ID: "young", Scope: scope, RecordClass: "context_payload", Purpose: "context-retention", Jurisdiction: "SG", MinimumAgeSeconds: 3600}} {
			if _, err := engine.RegisterRetentionPolicy(ctx, c, model.RetentionPolicyRequest{Command: command(scope + policy.ID), Policy: policy}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := normal.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: command("memory-type"), Definition: model.MemoryType{Scope: "case1", Subtype: "episode", Version: "v1", PolicyID: "manual", Classification: "confidential"}}); err != nil {
		t.Fatal(err)
	}
	memory, err := normal.ProposeMemory(ctx, c, model.MemoryProposal{Command: command("memory"), Key: model.MemoryKey{Scope: "case1", ID: "summary"}, Subtype: "episode", SchemaVersion: "v1", Body: "private retained memory", Valid: model.Interval{From: &now}, Sources: []model.SourceSpan{{EvidenceID: "case1-source", Start: 0, End: len(secret), Quote: secret}}, Dependencies: []model.MemoryDependency{dependency}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := normal.ApproveMemory(ctx, reviewer, model.Approval{Command: command("approve-memory"), Scope: "case1", RecordID: memory.RecordID, VersionID: memory.VersionID, ExpectedRevision: memory.StreamRevision}); err != nil {
		t.Fatal(err)
	}
	index.hits["case1"] = append(index.hits["case1"], model.SearchCandidate{Kind: "memory", RecordID: memory.RecordID, VersionID: memory.VersionID, Score: 2})
	if _, err := admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,$2,3)", c.Tenant, model.ProjectionConsumer("context-fixture")); err != nil {
		t.Fatal(err)
	}
	reader, err := facts.NewWithRetrieval(ledger, allow{}, store, model.RetrievalOptions{Index: index, Progress: ledger, Generation: "context-fixture"})
	if err != nil {
		t.Fatal(err)
	}
	prepare := func(id string, mixed bool) model.ContextManifest {
		t.Helper()
		m, err := reader.PrepareContext(ctx, c, model.ContextRequest{Command: command(id), Scope: "case1", IncludeMemories: mixed, Query: model.RecallRequest{Scopes: []string{"case1"}, Query: "private search query", Purpose: "context-retention", ValidAt: now, Limit: 20}})
		if err != nil {
			t.Fatal(err)
		}
		if len(m.Items) != 1 || m.Items[0].Fact.Value == nil || (mixed && (len(m.Memories) != 1 || len(model.ContextScopes(m)) != 2)) {
			t.Fatal("context fixture lacks content", m)
		}
		return m
	}
	one, multi, decision, exported := prepare("one", false), prepare("multi", true), prepare("decision", false), prepare("exported", false)
	request := func(m model.ContextManifest) model.ContextDisposalRequest {
		r := model.ContextDisposalRequest{Command: command("dispose-" + m.ID), ContextID: m.ID, Reason: "retention period expired"}
		for _, scope := range model.ContextScopes(m) {
			r.Bindings = append(r.Bindings, model.ContextRetentionBinding{Scope: scope, PolicyID: "contexts", Jurisdiction: "SG"})
		}
		return r
	}
	reject := func(e *facts.Engine, r model.ContextDisposalRequest, want error) {
		t.Helper()
		if _, err := e.DisposeContext(ctx, c, r); !errors.Is(err, want) {
			t.Fatalf("context disposal got %v want %v", err, want)
		}
	}
	batchOne, batchMulti := prepare("batch-one", false), prepare("batch-multi", true)
	batch := model.DisposalBatchRequest{Command: command("batch-contexts"), Scope: "case1", Reason: "retention period expired"}
	for _, m := range []model.ContextManifest{batchOne, batchMulti} {
		item := request(m)
		item.Command = model.Command{}
		item.Reason = ""
		batch.Contexts = append(batch.Contexts, item)
	}
	badBatch := batch
	badBatch.Contexts = append([]model.ContextDisposalRequest(nil), batch.Contexts...)
	badBatch.Contexts[1].Bindings = append([]model.ContextRetentionBinding(nil), batch.Contexts[1].Bindings...)
	badBatch.Contexts[1].Bindings[0].PolicyID = "young"
	if _, err := engine.DisposeBatch(ctx, c, badBatch); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("batch age policy", err)
	}
	for _, m := range []model.ContextManifest{batchOne, batchMulti} {
		if _, err := normal.GetContextManifest(ctx, c, model.ContextQuery{ID: m.ID, Purpose: "read"}); err != nil {
			t.Fatal("failed batch lost context", err)
		}
	}
	firstBatch, err := engine.DisposeBatch(ctx, c, batch)
	if err != nil {
		t.Fatal(err)
	}
	batch.Contexts[0], batch.Contexts[1] = batch.Contexts[1], batch.Contexts[0]
	testDisposalBatchRemote(t, ctx, engine, c, batch, firstBatch)
	for _, m := range []model.ContextManifest{batchOne, batchMulti} {
		if _, err := normal.GetContextManifest(ctx, c, model.ContextQuery{ID: m.ID, Purpose: "read"}); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("batch context retained", err)
		}
	}
	reject(normal, request(one), model.ErrDenied)
	bad := request(one)
	bad.Bindings[0].PolicyID = "young"
	reject(engine, bad, model.ErrPrecondition)
	bad = request(multi)
	bad.Bindings = bad.Bindings[:1]
	reject(engine, bad, model.ErrDenied)
	bad = request(one)
	bad.ContextID = "missing-context"
	reject(engine, bad, model.ErrDenied)
	bad = request(one)
	bad.Bindings[0].Scope = "case2"
	reject(engine, bad, model.ErrDenied)
	bad = request(one)
	bad.Bindings[0].Jurisdiction = "US"
	reject(engine, bad, model.ErrPrecondition)
	bad = request(one)
	bad.Purpose = "different-purpose"
	reject(engine, bad, model.ErrPrecondition)
	denied, _ := facts.New(ledger, denyScope{scope: "case2"}, store)
	reject(denied, request(multi), model.ErrDenied)
	hold := model.LegalHoldRequest{Command: command("hold"), ID: "context-hold", Scope: "case2", Reason: "review"}
	if _, err := engine.PlaceLegalHold(ctx, c, hold); err != nil {
		t.Fatal(err)
	}
	reject(engine, request(multi), model.ErrPrecondition)
	if _, err := engine.ReleaseLegalHold(ctx, c, model.ReleaseLegalHoldRequest{Command: command("release"), ID: hold.ID, Scope: hold.Scope, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,'archive-pending',3)", c.Tenant); err != nil {
		t.Fatal(err)
	}
	reject(engine, request(one), model.ErrPrecondition)
	if _, err := admin.Exec(ctx, "DELETE FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id='archive-pending'", c.Tenant); err != nil {
		t.Fatal(err)
	}
	artifact := model.ArtifactReference{}
	if err := ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		ev, err := tx.Evidence(ctx, "case1-source")
		artifact = model.ArtifactReference{EvidenceID: ev.ID, Digest: ev.Object.Digest}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	decisionRequest := func(m model.ContextManifest) model.DecisionReceipt {
		ref := model.VersionReference{RecordID: m.Items[0].Fact.RecordID, VersionID: m.Items[0].Fact.VersionID}
		return model.DecisionReceipt{Command: command("record-" + m.ID), Scope: "case1", ApplicationID: "app", ExternalDecisionID: m.ID, ContextID: m.ID, SelectedItems: []model.VersionReference{ref}, UsedItems: []model.VersionReference{ref}, MethodKind: "rule", MethodID: "rule", MethodVersion: "v1", TransformationVersion: "v1", Input: artifact, Output: artifact, DecidedAt: time.Now().UTC(), Outcome: "succeeded"}
	}
	if _, err := normal.RecordDecision(ctx, c, decisionRequest(decision)); err != nil {
		t.Fatal(err)
	}
	reject(engine, request(decision), model.ErrPrecondition)
	raced := prepare("raced", true)
	start := make(chan struct{})
	recorded := make(chan error, 1)
	go func() { <-start; _, err := normal.RecordDecision(ctx, c, decisionRequest(raced)); recorded <- err }()
	close(start)
	_, disposeErr := engine.DisposeContext(ctx, c, request(raced))
	decisionErr := <-recorded
	if (disposeErr == nil) == (decisionErr == nil) {
		t.Fatal("wrong race winners", disposeErr, decisionErr)
	}
	loser := disposeErr
	if loser == nil {
		loser = decisionErr
	}
	if !errors.Is(loser, model.ErrPrecondition) {
		t.Fatal("race error", loser)
	}
	failing, _ := facts.New(rollbackRetentionLedger{ledger}, allow{}, store)
	reject(failing, request(one), model.ErrUnavailable)
	if _, err := normal.GetContextManifest(ctx, c, model.ContextQuery{ID: one.ID, Purpose: "read"}); err != nil {
		t.Fatal("rollback lost context", err)
	}
	var original []byte
	if err := admin.QueryRow(ctx, "SELECT payload FROM context_payloads WHERE tenant_id=$1 AND context_id=$2", c.Tenant, one.ID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	verifyArchiveReplay := archiveRevisionsBeforeDisposal(t, ctx, admin, cipher, signer, map[string]ed25519.PublicKey{"context-key": private.Public().(ed25519.PublicKey)}, c.Tenant, one.CreationRevision, multi.CreationRevision)
	for _, m := range []model.ContextManifest{one, multi} {
		r := request(m)
		first, err := engine.DisposeContext(ctx, c, r)
		if err != nil {
			t.Fatal(err)
		}
		testContextDisposalRemote(t, ctx, engine, c, r, first)
		// Binding order is not part of command identity.
		if len(r.Bindings) == 2 {
			r.Bindings[0], r.Bindings[1] = r.Bindings[1], r.Bindings[0]
		}
		retry, err := engine.DisposeContext(ctx, c, r)
		if err != nil || retry != first {
			t.Fatal("replay", retry, err)
		}
		r.Reason = "changed"
		reject(engine, r, model.ErrConflict)
		got, err := normal.GetContextManifest(ctx, c, model.ContextQuery{ID: m.ID, Purpose: "read"})
		if !errors.Is(err, model.ErrPrecondition) || got.ID != "" {
			t.Fatal("disposed context disclosed", got, err)
		}
		if _, err := reader.PrepareContext(ctx, c, m.Request); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("prepare retry disclosed disposed content", err)
		}
		if _, err := normal.RecordDecision(ctx, c, decisionRequest(m)); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("decision used disposed content", err)
		}
		var count int
		if err := admin.QueryRow(ctx, "SELECT count(*) FROM context_payloads WHERE tenant_id=$1 AND context_id=$2", c.Tenant, m.ID).Scan(&count); err != nil || count != 0 {
			t.Fatal("context bytes retained", count, err)
		}
		if err := ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
			metadata, err := tx.(model.ContextTx).ContextManifest(ctx, m.ID)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(metadata, model.ContextTombstone(m)) {
				t.Fatal("wrong tombstone", metadata)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	verifyArchiveReplay()
	reject(denied, request(multi), model.ErrDenied)
	if got, err := normal.GetMemory(ctx, c, model.MemoryQuery{Key: model.MemoryKey{Scope: "case1", ID: "summary"}, ValidAt: now, Purpose: "read"}); err != nil || got.Body != "private retained memory" {
		t.Fatal("source memory changed", got, err)
	}
	if got, err := normal.GetFact(ctx, c, model.FactQuery{Key: model.FactKey{Scope: "case1", Subject: "subject", Predicate: "value"}, ValidAt: now, Purpose: "read"}); err != nil || got.Value == nil || got.Value.Text == nil || *got.Value.Text != secret {
		t.Fatal("source fact changed", got, err)
	}
	for _, query := range []string{"UPDATE context_disposals SET payload=decode('00','hex')", "DELETE FROM context_disposals", "TRUNCATE context_disposals"} {
		_, err := pool.Exec(ctx, query)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Fatal("retention role modified tombstone", err)
		}
	}
	other := c
	other.Tenant = "other-tenant"
	if _, err := normal.GetContextManifest(ctx, other, model.ContextQuery{ID: one.ID, Purpose: "read"}); !errors.Is(err, model.ErrDenied) {
		t.Fatal("cross-tenant context disclosed", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := ledgerdb.New(tx).BindTenant(ctx, c.Tenant); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "INSERT INTO context_payloads(tenant_id,context_id,payload) VALUES($1,$2,$3)", c.Tenant, one.ID, original)
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "23514" {
		t.Fatal("context resurrected", err)
	}
	_ = tx.Rollback(ctx)
	job, err := engine.StartAuditExport(ctx, c, model.AuditExportRequest{Command: command("export"), Scope: "case1", IncludeEvidence: true})
	if err != nil {
		t.Fatal(err)
	}
	reject(engine, request(exported), model.ErrPrecondition)
	result, err := engine.RunAuditExport(ctx, c, model.AuditExportQuery{ID: job.ID, Scope: "case1", Purpose: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	gap := false
	for _, g := range result.Bundle.Manifest.Gaps {
		if g.Kind == "context_content" && g.ID == one.ID {
			gap = true
		}
	}
	if !gap {
		t.Fatal("context export gap missing")
	}
	for _, entry := range result.Bundle.Entries {
		if entry.Kind == "context" {
			var m model.ContextManifest
			if err := json.Unmarshal(entry.Payload, &m); err != nil {
				t.Fatal(err)
			}
			if m.ID == multi.ID {
				t.Fatal("cross-scope tombstone disclosed")
			}
			if m.ID == one.ID && !reflect.DeepEqual(m, model.ContextTombstone(one)) {
				t.Fatal("export reconstructed content")
			}
		}
	}
	changes, err := normal.ListChanges(ctx, c, model.ChangesQuery{Scope: "case1", Purpose: "archive", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range changes.Events {
		if event.LedgerRevision == one.CreationRevision {
			payload, err := ledger.ArchivePayload(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: c.Tenant, Consumer: "archive-fixture"}, Event: event})
			if err != nil {
				t.Fatal(err)
			}
			for _, g := range payload.Gaps {
				if g.Kind == "context_content" && g.ID == one.ID {
					return
				}
			}
			t.Fatal("archive context gap missing")
		}
	}
	t.Fatal("context creation event missing")
}
