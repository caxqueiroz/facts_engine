package postgres_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"slices"
	"testing"

	facts "github.com/caxqueiroz/facts_engine"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/evidence"
	"github.com/caxqueiroz/facts_engine/model"
)

func testMemoryProposals(t *testing.T, ctx context.Context, l model.Ledger, store *evidence.Postgres, c model.Caller, source model.FactProposal, fact model.CommitResult) {
	t.Helper()
	engine, err := facts.New(l, allow{}, store)
	if err != nil {
		t.Fatal(err)
	}
	cmd := func(key string) model.Command { return model.Command{Key: key, Purpose: "memory-review"} }
	definition := model.MemoryType{Scope: source.Key.Scope, Subtype: "episode", Version: "v1", PolicyID: "manual1", Classification: "confidential"}
	if _, err = engine.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: cmd("memory-type"), Definition: definition}); err != nil {
		t.Fatal(err)
	}
	request := model.MemoryProposal{Command: cmd("memory-proposal"), Key: model.MemoryKey{Scope: source.Key.Scope, ID: "conversation-summary"}, Subtype: "episode", SchemaVersion: "v1", Body: "Customer discussed a preference for tea.", Valid: source.Valid, Sources: source.Sources, Dependencies: []model.MemoryDependency{{Scope: source.Key.Scope, RecordID: fact.RecordID, VersionID: fact.VersionID, ContentDigest: fact.ContentDigest, Label: "assertion"}}}
	result, err := engine.ProposeMemory(ctx, c, request)
	if err != nil || result.RecordKind != "memory" || result.StreamRevision != 1 {
		t.Fatal(result, err)
	}
	retry, err := engine.ProposeMemory(ctx, c, request)
	if err != nil || retry != result {
		t.Fatal("memory retry", retry, err)
	}
	changed := request
	changed.Body = "changed"
	if _, err = engine.ProposeMemory(ctx, c, changed); !errors.Is(err, model.ErrConflict) {
		t.Fatal("changed command", err)
	}
	query := model.MemoryVersionQuery{Scope: request.Key.Scope, RecordID: result.RecordID, VersionID: result.VersionID, Purpose: "memory-review"}
	retained, err := engine.GetMemoryVersion(ctx, c, query)
	if err != nil || retained.Proposal.Body != request.Body || retained.Digest != result.ContentDigest || len(retained.Authorization) != 2 {
		t.Fatal(retained, err)
	}
	other := c
	other.Tenant = "other-tenant"
	if _, err = engine.GetMemoryVersion(ctx, other, query); !errors.Is(err, model.ErrDenied) {
		t.Fatal("tenant boundary", err)
	}
	denied, _ := facts.New(l, denyPermission{permission: "can_read_history"}, store)
	if _, err = denied.GetMemoryVersion(ctx, c, query); !errors.Is(err, model.ErrDenied) {
		t.Fatal("history permission", err)
	}
	changed = request
	changed.Command = cmd("memory-bad-dependency")
	changed.Dependencies = append([]model.MemoryDependency(nil), request.Dependencies...)
	changed.Dependencies[0].ContentDigest = model.BytesDigest([]byte("wrong"))
	if _, err = engine.ProposeMemory(ctx, c, changed); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("dependency digest bypass", err)
	}
	changed = request
	changed.Command = cmd("memory-stale")
	changed.Body = "second proposal"
	if _, err = engine.ProposeMemory(ctx, c, changed); !errors.Is(err, model.ErrRevision) {
		t.Fatal("memory revision bypass", err)
	}
	changed.ExpectedRevision = 1
	next, err := engine.ProposeMemory(ctx, c, changed)
	if err != nil || next.RecordID != result.RecordID || next.VersionID == result.VersionID {
		t.Fatal("memory correction", next, err)
	}
	if _, err = engine.GetFactHistory(ctx, c, model.HistoryQuery{Scope: query.Scope, RecordID: query.RecordID, Purpose: query.Purpose}); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("memory presented as fact history", err)
	}
	if err = l.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		record, err := tx.Record(ctx, result.RecordID)
		if err != nil {
			return err
		}
		if record.Kind != "memory" || record.Revision != 2 {
			t.Fatal(record)
		}
		_, err = tx.Version(ctx, result.RecordID, result.VersionID)
		if !errors.Is(err, model.ErrNotFound) {
			t.Fatal("memory exposed as typed fact", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, err := engine.GetRetentionStatus(ctx, c, model.RetentionStatusQuery{Scope: request.Key.Scope, EvidenceID: source.Sources[0].EvidenceID, Purpose: "memory-review"})
	if err != nil || !slices.Contains(status.Blockers, "memory_versions") {
		t.Fatal("memory evidence disposal not blocked", status, err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := audit.NewEd25519("memory-test", private)
	if err != nil {
		t.Fatal(err)
	}
	exporter, err := facts.NewWithAudit(l, allow{}, store, model.AuditOptions{Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	for _, include := range []bool{false, true} {
		key := "memory-metadata-audit"
		if include {
			key = "memory-full-audit"
		}
		job, err := exporter.StartAuditExport(ctx, c, model.AuditExportRequest{Command: cmd(key), Scope: request.Key.Scope, IncludeEvidence: include})
		if err != nil {
			t.Fatal(err)
		}
		done, err := exporter.RunAuditExport(ctx, c, model.AuditExportQuery{ID: job.ID, Scope: request.Key.Scope, Purpose: "memory-review"})
		if err != nil {
			t.Fatal(err)
		}
		if err = audit.Verify(*done.Bundle, map[string]ed25519.PublicKey{"memory-test": public}); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, entry := range done.Bundle.Entries {
			if entry.Kind == "memory_version" && entry.ID == result.VersionID {
				found = true
			}
		}
		if found != include {
			t.Fatal("memory export payload disclosure", include, found)
		}
	}

}
