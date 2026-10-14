package postgres_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"slices"
	"testing"
	"time"

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
	reviewer := c
	reviewer.Principal = "memory-reviewer"
	approval := model.Approval{Command: cmd("memory-approval"), Scope: query.Scope, RecordID: result.RecordID, VersionID: result.VersionID, ExpectedRevision: 2}
	if _, err = engine.ApproveMemory(ctx, c, approval); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("self approval", err)
	}
	accepted, err := engine.ApproveMemory(ctx, reviewer, approval)
	if err != nil {
		t.Fatal(err)
	}
	read := model.MemoryQuery{Key: request.Key, Purpose: "memory-review", ValidAt: *source.Valid.From}
	current, err := engine.GetMemory(ctx, reviewer, read)
	if err != nil || current.State != "resolved" || current.Body != request.Body {
		t.Fatal(current, err)
	}
	later := source.Valid.From.Add(time.Hour)
	factNow, err := engine.GetFact(ctx, reviewer, model.FactQuery{Key: source.Key, ValidAt: *source.Valid.From})
	if err != nil {
		t.Fatal(err)
	}
	factChange := model.LifecycleChange{Approval: model.Approval{Command: cmd("memory-dependency-dispute"), Scope: source.Key.Scope, RecordID: fact.RecordID, VersionID: fact.VersionID, ExpectedRevision: factNow.StreamRevision}, Valid: model.Interval{From: &later}, Reason: "dependency_review"}
	disputed, err := engine.DisputeFact(ctx, reviewer, factChange)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := engine.GetMemory(ctx, reviewer, read)
	if err != nil || stale.State != "stale" || stale.Body != "" {
		t.Fatal("dependency change outside query instant ignored", stale, err)
	}
	historicalQuery := read
	historicalQuery.KnowledgeRevision = accepted.LedgerRevision
	historical, err := engine.GetMemory(ctx, reviewer, historicalQuery)
	if err != nil || historical.State != "resolved" || historical.Body != request.Body {
		t.Fatal("historical memory changed", historical, err)
	}
	factChange.Command = cmd("memory-dependency-resolve")
	factChange.ExpectedRevision = disputed.StreamRevision
	if _, err = engine.ResolveDispute(ctx, reviewer, factChange); err != nil {
		t.Fatal(err)
	}
	change := model.LifecycleChange{Approval: approval, Valid: model.Interval{From: &later}, Reason: "memory_review"}
	change.Command = cmd("memory-dispute")
	change.ExpectedRevision = 3
	if _, err = engine.DisputeMemory(ctx, reviewer, change); err != nil {
		t.Fatal(err)
	}
	laterQuery := read
	laterQuery.ValidAt = later
	current, err = engine.GetMemory(ctx, reviewer, laterQuery)
	if err != nil || current.State != "disputed" || current.Body != "" {
		t.Fatal(current, err)
	}
	change.Command = cmd("memory-resolve")
	change.ExpectedRevision = 4
	if _, err = engine.ResolveMemoryDispute(ctx, reviewer, change); err != nil {
		t.Fatal(err)
	}
	replacement := model.Supersession{Approval: model.Approval{Command: cmd("memory-supersede"), Scope: query.Scope, RecordID: result.RecordID, VersionID: next.VersionID, ExpectedRevision: 5}, Valid: request.Valid, ReplacedVersionIDs: []string{result.VersionID}, Reason: "memory_correction"}
	if _, err = engine.ApproveMemory(ctx, reviewer, replacement.Approval); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("overlap accepted", err)
	}
	if _, err = engine.SupersedeMemory(ctx, reviewer, replacement); err != nil {
		t.Fatal(err)
	}
	current, err = engine.GetMemory(ctx, reviewer, read)
	if err != nil || current.Body != "second proposal" {
		t.Fatal(current, err)
	}
	change.Approval = replacement.Approval
	change.Command = cmd("memory-retract")
	change.ExpectedRevision = 6
	change.Valid = request.Valid
	if _, err = engine.RetractMemory(ctx, reviewer, change); err != nil {
		t.Fatal(err)
	}
	current, err = engine.GetMemory(ctx, reviewer, read)
	if err != nil || current.State != "retracted" || current.Body != "" {
		t.Fatal(current, err)
	}
	changed.Command = cmd("memory-rejected-proposal")
	changed.ExpectedRevision = 7
	unaccepted, err := engine.ProposeMemory(ctx, c, changed)
	if err != nil {
		t.Fatal(err)
	}
	change.Command = cmd("memory-reject")
	change.VersionID = unaccepted.VersionID
	change.ExpectedRevision = 8
	if _, err = engine.RejectMemory(ctx, reviewer, change); err != nil {
		t.Fatal(err)
	}
	change.Command = cmd("memory-accept-rejected")
	change.ExpectedRevision = 9
	if _, err = engine.ApproveMemory(ctx, reviewer, change.Approval); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("rejected memory accepted", err)
	}
	for _, required := range []bool{false, true} {
		policyID := "memory-auto"
		if required {
			policyID = "memory-auto-required"
		}
		policy := model.AcceptancePolicy{Scope: query.Scope, ID: policyID, Mode: "automated", AcceptingServices: []string{"memory-service"}, SourceClasses: []string{"customer"}}
		if required {
			policy.Required = []model.AttestationRequirement{{Kind: "application_validation", Principals: []string{c.Principal}}}
		}
		if _, err = engine.RegisterAcceptancePolicy(ctx, c, model.PolicyRequest{Command: cmd(policyID), Policy: policy}); err != nil {
			t.Fatal(err)
		}
		autoType := definition
		autoType.Subtype = policyID
		autoType.PolicyID = policyID
		if _, err = engine.RegisterMemoryType(ctx, c, model.MemoryTypeRequest{Command: cmd(policyID + "-type"), Definition: autoType}); err != nil {
			t.Fatal(err)
		}
		autoRequest := request
		autoRequest.Command = cmd(policyID + "-proposal")
		autoRequest.Key.ID = policyID
		autoRequest.Subtype = policyID
		autoRequest.Dependencies = nil
		autoVersion, err := engine.ProposeMemory(ctx, c, autoRequest)
		if err != nil {
			t.Fatal(err)
		}
		autoApproval := model.Approval{Command: cmd(policyID + "-accept"), Scope: query.Scope, RecordID: autoVersion.RecordID, VersionID: autoVersion.VersionID, ExpectedRevision: 1}
		if _, err = engine.ApproveMemory(ctx, reviewer, autoApproval); !errors.Is(err, model.ErrPrecondition) {
			t.Fatal("automated policy accepted manually", err)
		}
		service := model.Caller{Tenant: c.Tenant, Principal: "memory-service", Service: true}
		_, err = engine.AcceptMemory(ctx, service, autoApproval)
		if required {
			if !errors.Is(err, model.ErrPrecondition) {
				t.Fatal("memory prerequisites bypassed", err)
			}
			testMemoryAttestations(t, ctx, engine, c, reviewer, service, autoRequest, autoVersion, autoApproval)
		} else if err != nil {
			t.Fatal("configured automatic acceptance failed", err)
		}
	}
	status, err := engine.GetRetentionStatus(ctx, c, model.RetentionStatusQuery{Scope: request.Key.Scope, EvidenceID: source.Sources[0].EvidenceID, Purpose: "memory-review"})
	if err != nil || (!slices.Contains(status.Blockers, "memory_versions") || !slices.Contains(status.Blockers, "memory_attestations")) {
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
