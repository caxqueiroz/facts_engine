package workers

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/adapters/archive"
	"github.com/caxqueiroz/facts_engine/adapters/audit"
	"github.com/caxqueiroz/facts_engine/adapters/postgres"
	"github.com/caxqueiroz/facts_engine/model"
)

type mutableArchiveSource struct{ payload model.ArchivePayload }

func (s *mutableArchiveSource) ArchivePayload(context.Context, model.Delivery) (model.ArchivePayload, error) {
	return s.payload, nil
}

type alteredArchiveRead struct{ model.ArchiveStore }

func (s alteredArchiveRead) Read(context.Context, string, model.ObjectRef) ([]byte, error) {
	return []byte("altered"), nil
}
func archiveRetentionFixture(t *testing.T) (model.ArchivePayload, model.ArchivePayload) {
	t.Helper()
	value := "original snapshot content"
	snapshot := model.FactSnapshot{ID: "snapshot", Tenant: "tenant", Caller: model.Caller{Tenant: "tenant", Principal: "reader"}, Purpose: "read", CreationRevision: 1, CanonicalVersion: "fact-snapshot-v1", Items: []model.SnapshotItem{{Selector: model.FactKey{Scope: "scope", Subject: "subject", Predicate: "predicate"}, Resolution: model.FactResult{State: "resolved", Value: &model.Value{Kind: model.Text, Text: &value}}}}}
	snapshot.Digest = model.SnapshotDigest(snapshot)
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	before := model.ArchivePayload{Event: model.Event{ID: "event", Type: "create_fact_snapshot", Scope: "scope", Caller: snapshot.Caller, LedgerRevision: 1}, Scopes: []string{"scope"}, Entries: []model.AuditEntry{{Kind: "snapshot", ID: snapshot.ID, Payload: raw}}, Gaps: []model.AuditGap{{Kind: "evidence_content", Reason: "locators_only"}}}
	after := cloneArchivePayload(t, before)
	snapshot.PayloadDisposed = true
	snapshot.Items[0].Resolution.Value = nil
	raw, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	after.Entries[0].Payload = raw
	after.Gaps = append([]model.AuditGap{{Kind: "snapshot_content", ID: snapshot.ID, Reason: "disposed_under_retention_policy"}}, after.Gaps...)
	return before, after
}
func cloneArchivePayload(t *testing.T, p model.ArchivePayload) model.ArchivePayload {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var out model.ArchivePayload
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func TestArchiveRetentionTransitionBoundaries(t *testing.T) {
	before, after := archiveRetentionFixture(t)
	raw, _ := json.Marshal(before)
	if !archiveRetentionReplay(raw, after) {
		t.Fatal("valid disposal transition rejected")
	}
	changeSnapshot := func(p *model.ArchivePayload, change func(*model.FactSnapshot)) {
		var s model.FactSnapshot
		if err := json.Unmarshal(p.Entries[0].Payload, &s); err != nil {
			t.Fatal(err)
		}
		change(&s)
		p.Entries[0].Payload, _ = json.Marshal(s)
	}
	cases := map[string]func(*model.ArchivePayload){
		"different event": func(p *model.ArchivePayload) { p.Event.Reason = "changed" },
		"different scope": func(p *model.ArchivePayload) { p.Scopes = []string{"other"} },
		"different entry": func(p *model.ArchivePayload) { p.Entries[0].ID = "other" },
		"missing gap":     func(p *model.ArchivePayload) { p.Gaps = p.Gaps[1:] },
		"extra gap":       func(p *model.ArchivePayload) { p.Gaps = append(p.Gaps, model.AuditGap{Kind: "new", Reason: "unknown"}) },
		"duplicate gap":   func(p *model.ArchivePayload) { p.Gaps = append(p.Gaps, p.Gaps[0]) },
		"changed gap":     func(p *model.ArchivePayload) { p.Gaps[1].Reason = "changed" },
		"different digest": func(p *model.ArchivePayload) {
			changeSnapshot(p, func(s *model.FactSnapshot) { s.Digest = model.BytesDigest(nil) })
		},
		"different caller": func(p *model.ArchivePayload) {
			changeSnapshot(p, func(s *model.FactSnapshot) { s.Caller.Principal = "other" })
		},
		"different selection": func(p *model.ArchivePayload) {
			changeSnapshot(p, func(s *model.FactSnapshot) { s.SelectionRevision++ })
		},
		"content retained": func(p *model.ArchivePayload) {
			changeSnapshot(p, func(s *model.FactSnapshot) { s.Items[0].Resolution.Value = &model.Value{Kind: model.Boolean} })
		},
		"not disposed": func(p *model.ArchivePayload) {
			changeSnapshot(p, func(s *model.FactSnapshot) { s.PayloadDisposed = false })
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			current := cloneArchivePayload(t, after)
			change(&current)
			if archiveRetentionReplay(raw, current) {
				t.Fatal("unrelated change accepted")
			}
		})
	}
	bad := cloneArchivePayload(t, before)
	changeSnapshot(&bad, func(s *model.FactSnapshot) { s.Items[0].Resolution.Value = &model.Value{Kind: model.Boolean} })
	badRaw, _ := json.Marshal(bad)
	if archiveRetentionReplay(badRaw, after) {
		t.Fatal("original canonical digest not verified")
	}
	if archiveRetentionReplay(raw, before) {
		t.Fatal("non-retention difference accepted")
	}
}
func TestArchiveRetryAfterSnapshotDisposal(t *testing.T) {
	before, after := archiveRetentionFixture(t)
	testArchiveRetentionRetry(t, before, after)
}
func TestArchiveRetryAfterContextDisposal(t *testing.T) {
	before, after := contextRetentionFixture(t)
	testArchiveRetentionRetry(t, before, after)
}
func testArchiveRetentionRetry(t *testing.T, before, after model.ArchivePayload) {
	t.Helper()
	ctx := context.Background()
	source := &mutableArchiveSource{payload: before}
	cipher, _ := postgres.NewAESGCM(bytes.Repeat([]byte{7}, 32))
	store, err := archive.NewFiles(t.TempDir(), cipher)
	if err != nil {
		t.Fatal(err)
	}
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := audit.NewEd25519("key", private)
	keys := map[string]ed25519.PublicKey{"key": public}
	caller := model.Caller{Tenant: "tenant", Principal: "archiver", Service: true}
	auth := fixedWorkerAuth{model.AuthorizationDecision{Allowed: true, ModelID: "model", Consistency: "HIGHER_CONSISTENCY", CheckedAt: time.Now()}}
	worker, err := NewArchive(source, store, signer, keys, auth, caller, "stream")
	if err != nil {
		t.Fatal(err)
	}
	delivery := model.Delivery{Key: model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.ArchiveConsumer("stream")}, Event: before.Event}
	if err := worker.Handle(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	head, manifest, err := store.Load(ctx, caller.Tenant, archive.ManifestKey("stream", 1))
	if err != nil {
		t.Fatal(err)
	}
	source.payload = after
	for range 2 {
		if err := worker.Handle(ctx, delivery); err != nil {
			t.Fatal("retention retry", err)
		}
	}
	gotHead, gotManifest, err := store.Load(ctx, caller.Tenant, archive.ManifestKey("stream", 1))
	if err != nil || gotHead != head || !bytes.Equal(gotManifest, manifest) {
		t.Fatal("retention retry rewrote archive", err)
	}
	if err := archive.VerifyChain(ctx, store, caller.Tenant, "stream", head, 1, 1, keys); err != nil {
		t.Fatal(err)
	}
	source.payload = cloneArchivePayload(t, after)
	source.payload.Event.Reason = "unrelated"
	if err := worker.Handle(ctx, delivery); !errors.Is(err, model.ErrConflict) {
		t.Fatal("unrelated archive change accepted", err)
	}
	source.payload = after
	tampered, err := NewArchive(source, alteredArchiveRead{store}, signer, keys, auth, caller, "stream")
	if err != nil {
		t.Fatal(err)
	}
	if err := tampered.Handle(ctx, delivery); !errors.Is(err, model.ErrPrecondition) {
		t.Fatal("altered stored bytes accepted", err)
	}
	auth.decision.Allowed = false
	denied, err := NewArchive(source, store, signer, keys, auth, caller, "stream")
	if err != nil {
		t.Fatal(err)
	}
	if err := denied.Handle(ctx, delivery); !errors.Is(err, model.ErrDenied) {
		t.Fatal("revoked archive permission ignored", err)
	}
}
