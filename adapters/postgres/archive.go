package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/caxqueiroz/facts_engine/model"
	"sort"
)

// ArchivePayload retains the full event and immutable referenced artifacts.
// Missing classes are explicit gaps; this is not a complete database backup.
func (l *Ledger) ArchivePayload(ctx context.Context, d model.Delivery) (model.ArchivePayload, error) {
	p := model.ArchivePayload{}
	err := l.Transact(ctx, d.Key.Tenant, func(tx model.Tx) error {
		t := tx.(*transaction)
		p = model.ArchivePayload{}
		var b []byte
		if err := t.tx.QueryRow(ctx, `SELECT payload,recorded_at FROM ledger_events WHERE tenant_id=$1 AND id=$2 AND ledger_revision=$3`, d.Key.Tenant, d.Event.ID, d.Event.LedgerRevision).Scan(&b, &p.Event.RecordedAt); err != nil {
			return err
		}
		at := p.Event.RecordedAt
		if err := t.decode(b, &p.Event); err != nil {
			return err
		}
		p.Event.RecordedAt = at.UTC()
		e := p.Event
		if e.Scope != d.Event.Scope || e.Caller.Tenant != d.Key.Tenant {
			return model.ErrPrecondition
		}
		add := func(kind, id string, v any) error {
			if kind == "evidence" {
				for _, entry := range p.Entries {
					if entry.Kind == kind && entry.ID == id {
						return nil
					}
				}
			}
			if evidence, ok := v.(model.Evidence); ok && evidence.Derivation != nil {
				parent, err := tx.Evidence(ctx, evidence.Derivation.ParentEvidenceID)
				if err != nil {
					return err
				}
				if parent.Scope != evidence.Scope {
					return model.ErrPrecondition
				}
				payload, err := json.Marshal(parent)
				if err != nil {
					return err
				}
				found := false
				for _, entry := range p.Entries {
					if entry.Kind == "evidence" && entry.ID == parent.ID {
						found = true
						break
					}
				}
				if !found {
					p.Entries = append(p.Entries, model.AuditEntry{Kind: "evidence", ID: parent.ID, Payload: payload})
				}
			}

			raw, err := json.Marshal(v)
			if err != nil {
				return err
			}
			p.Entries = append(p.Entries, model.AuditEntry{Kind: kind, ID: id, Payload: raw})
			return nil
		}
		scopes := map[string]bool{e.Scope: true}
		// Every event contains its complete retained envelope. Referenced fact
		// versions include exact evidence locators, schemas and policy versions.
		if e.RecordKind != "memory" && e.RecordID != "" && e.VersionID != "" {
			v, err := tx.Version(ctx, e.RecordID, e.VersionID)
			if err != nil {
				return err
			}
			if err = add("version", v.ID, v); err != nil {
				return err
			}
			schema, err := tx.Predicate(ctx, e.Scope, v.Proposal.Key.Predicate, v.Proposal.SchemaVersion)
			if err != nil {
				return err
			}
			if err = add("predicate", schema.Predicate+"/"+schema.Version, schema); err != nil {
				return err
			}
			policy, err := tx.Policy(ctx, e.Scope, v.PolicyID)
			if err != nil {
				return err
			}
			if err = add("policy", policy.ID, policy); err != nil {
				return err
			}
			seen := map[string]bool{}
			for _, span := range v.Proposal.Sources {
				if seen[span.EvidenceID] {
					continue
				}
				seen[span.EvidenceID] = true
				ev, err := tx.Evidence(ctx, span.EvidenceID)
				if err != nil {
					return err
				}
				if err = add("evidence", ev.ID, ev); err != nil {
					return err
				}
			}
		}
		if e.RecordKind == "memory" {
			memory, err := t.MemoryVersion(ctx, e.RecordID, e.VersionID)
			if err != nil {
				return err
			}
			if e.Type != "propose_memory" {
				if err = add("memory_version", memory.ID, memory); err != nil {
					return err
				}
			}
			definition, err := t.MemoryType(ctx, e.Scope, memory.Proposal.Subtype, memory.Proposal.SchemaVersion)
			if err != nil {
				return err
			}
			if err = add("memory_type", definition.Subtype+"/"+definition.Version, definition); err != nil {
				return err
			}
			policy, err := tx.Policy(ctx, e.Scope, memory.PolicyID)
			if err != nil {
				return err
			}
			if err = add("policy", policy.ID, policy); err != nil {
				return err
			}
			for _, dependency := range memory.Proposal.Dependencies {
				scopes[dependency.Scope] = true
			}
			seen := map[string]bool{}
			for _, span := range memory.Proposal.Sources {
				if seen[span.EvidenceID] {
					continue
				}
				seen[span.EvidenceID] = true
				evidence, err := tx.Evidence(ctx, span.EvidenceID)
				if err != nil {
					return err
				}
				if err = add("evidence", evidence.ID, evidence); err != nil {
					return err
				}
			}
		}
		switch e.Type {
		case "register_policy":
			v, err := tx.Policy(ctx, e.Scope, e.EntityID)
			if err != nil {
				return err
			}
			if err = add("policy", v.ID, v); err != nil {
				return err
			}
		case "register_evidence", "register_redacted_evidence":
			v, err := tx.Evidence(ctx, e.EntityID)
			if err != nil {
				return err
			}
			if err = add("evidence", v.ID, v); err != nil {
				return err
			}
		case "register_predicate":
			if e.DefinitionPredicate == "" {
				p.Gaps = append(p.Gaps, model.AuditGap{Kind: "predicate", ID: e.EntityID, Reason: "legacy_event_lacks_predicate_identity"})
			} else {
				v, err := tx.Predicate(ctx, e.Scope, e.DefinitionPredicate, e.EntityID)
				if err != nil {
					return err
				}
				if err = add("predicate", v.Predicate+"/"+v.Version, v); err != nil {
					return err
				}
			}
		}
		queries := []struct{ kind, sql string }{
			{"scope_identity", `SELECT scope_id||'/'||revision::text,payload FROM scope_versions WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"subject_identity", `SELECT subject_id||'/'||revision::text,payload FROM subject_versions WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"memory_timeline", `SELECT record_id||'/'||ordinal::text,payload FROM memory_timeline_history WHERE tenant_id=$1 AND ledger_revision=$2 ORDER BY record_id,ordinal`},
			{"memory_type", `SELECT subtype||'/'||version,payload FROM memory_types WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"memory_version", `SELECT id,payload FROM memory_versions WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"extraction_attempt", `SELECT id,payload FROM extraction_attempts WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"extraction_result", `SELECT attempt_id,payload FROM extraction_results WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"extraction_job", `SELECT id,payload FROM extraction_jobs WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"extraction_retry", `SELECT id,payload FROM extraction_retry_events WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"conversation", `SELECT id,payload FROM conversations WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"retention", `SELECT id,payload FROM retention_events WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"context", `SELECT id,payload FROM context_manifests WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"decision", `SELECT id,payload FROM decision_receipts WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"snapshot", `SELECT id,payload FROM fact_snapshots WHERE tenant_id=$1 AND creation_revision=$2`},
			{"calculation", `SELECT id,payload FROM calculation_receipts WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"memory_attestation", `SELECT a.id,a.payload FROM memory_attestations a WHERE a.tenant_id=$1 AND (a.ledger_revision=$2 OR EXISTS(SELECT 1 FROM memory_attestation_events w WHERE w.tenant_id=a.tenant_id AND w.attestation_id=a.id AND w.ledger_revision=$2))`},
			{"attestation", `SELECT id,payload FROM attestations WHERE tenant_id=$1 AND ledger_revision=$2`},
			{"timeline", `SELECT record_id||'/'||ordinal::text,payload FROM timeline_history WHERE tenant_id=$1 AND ledger_revision=$2 ORDER BY record_id,ordinal`},
			{"audit_export", `SELECT id,payload FROM audit_exports WHERE tenant_id=$1 AND ledger_revision=$2`},
		}
		total := 0
		for _, q := range queries {
			rows, err := t.tx.Query(ctx, q.sql, d.Key.Tenant, e.LedgerRevision)
			if err != nil {
				return err
			}
			for rows.Next() {
				var id string
				var b []byte
				if err = rows.Scan(&id, &b); err != nil {
					rows.Close()
					return err
				}
				plain, err := t.cipher.Open(d.Key.Tenant, b)
				if err != nil {
					rows.Close()
					return err
				}
				total += len(plain)
				if total > 60<<20 || len(p.Entries) > 10000 {
					rows.Close()
					return model.ErrPrecondition
				}
				p.Entries = append(p.Entries, model.AuditEntry{Kind: q.kind, ID: id, Payload: plain})
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}
		// Snapshots can span scopes. Resolve every referenced scope before the
		// worker authorizes disclosure to its external archive destination.
		for _, entry := range p.Entries {
			switch entry.Kind {
			case "memory_attestation", "attestation":
				var attestation model.Attestation
				if err := json.Unmarshal(entry.Payload, &attestation); err != nil {
					return err
				}
				scopes[attestation.Scope] = true
				for _, evidenceID := range model.AttestationEvidence(attestation) {
					present := false
					for _, existing := range p.Entries {
						if existing.Kind == "evidence" && existing.ID == evidenceID {
							present = true
							break
						}
					}
					if !present {
						evidence, err := tx.Evidence(ctx, evidenceID)
						if err != nil {
							return err
						}
						if err = add("evidence", evidence.ID, evidence); err != nil {
							return err
						}
					}
				}
			case "extraction_job", "extraction_attempt", "extraction_result":
				var artifact struct{ RefreshScopes, ContentScopes []string }
				if err := json.Unmarshal(entry.Payload, &artifact); err != nil {
					return err
				}
				for _, scope := range append(artifact.RefreshScopes, artifact.ContentScopes...) {
					scopes[scope] = true
				}
			case "context":
				var m model.ContextManifest
				if err := json.Unmarshal(entry.Payload, &m); err != nil {
					return err
				}
				for _, scope := range model.ContextScopes(m) {
					scopes[scope] = true
				}
			case "decision":
				var d model.DecisionRecord
				if err := json.Unmarshal(entry.Payload, &d); err != nil {
					return err
				}
				for _, scope := range d.Scopes {
					scopes[scope] = true
				}

			case "snapshot":
				var snapshot model.FactSnapshot
				if err := json.Unmarshal(entry.Payload, &snapshot); err != nil {
					return err
				}
				for _, item := range snapshot.Items {
					scopes[item.Selector.Scope] = true
				}
			case "calculation":
				var calculation model.CalculationRecord
				if err := json.Unmarshal(entry.Payload, &calculation); err != nil {
					return err
				}
				for _, id := range append([]string{calculation.Receipt.SnapshotID}, calculation.Receipt.ReferenceSnapshotIDs...) {
					var snapshot model.FactSnapshot
					if err := t.read(ctx, &snapshot, `SELECT payload FROM fact_snapshots WHERE tenant_id=$1 AND id=$2`, d.Key.Tenant, id); err != nil {
						return err
					}
					for _, item := range snapshot.Items {
						scopes[item.Selector.Scope] = true
					}
				}
				for _, ref := range []model.ArtifactReference{calculation.Receipt.Input, calculation.Receipt.Output} {
					ev, err := tx.Evidence(ctx, ref.EvidenceID)
					if err != nil {
						return err
					}
					scopes[ev.Scope] = true
				}
			}
		}
		for scope := range scopes {
			p.Scopes = append(p.Scopes, scope)
		}
		sort.Strings(p.Scopes)
		p.Gaps = append(p.Gaps, model.AuditGap{Kind: "evidence_content", Reason: "locators_only"}, model.AuditGap{Kind: "database_restore", Reason: "event_archive_not_database_backup"})
		sort.Slice(p.Entries, func(i, j int) bool {
			a, b := p.Entries[i], p.Entries[j]
			return fmt.Sprint(a.Kind, "\x00", a.ID) < fmt.Sprint(b.Kind, "\x00", b.ID)
		})
		return nil
	})
	return p, err
}
