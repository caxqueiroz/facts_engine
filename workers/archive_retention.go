package workers

import (
	"bytes"
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/model"
	"io"
)

// archiveRetentionReplay permits only a full version, snapshot or context transition to
// its exact metadata-only tombstone. All other entries, event fields, scopes
// and coverage gaps must still match the previously signed payload.
func archiveRetentionReplay(retained []byte, current model.ArchivePayload) bool {
	var prior model.ArchivePayload
	if json.Unmarshal(retained, &prior) != nil || len(prior.Entries) != len(current.Entries) {
		return false
	}
	normalized := current
	normalized.Entries = append([]model.AuditEntry(nil), current.Entries...)
	transitions := map[string]bool{}
	for i, entry := range current.Entries {
		old := prior.Entries[i]
		if entry.Kind != old.Kind || entry.ID != old.ID {
			return false
		}
		if model.BytesDigest(entry.Payload) == model.BytesDigest(old.Payload) {
			continue
		}
		key := entry.Kind + "_content:" + entry.ID
		if transitions[key] {
			return false
		}
		switch entry.Kind {
		case "version":
			if !versionRetentionTransition(old, entry, current.Event) {
				return false
			}
		case "memory_version":
			if !memoryVersionRetentionTransition(old, entry, current.Event) {
				return false
			}
		case "snapshot":
			if !snapshotRetentionTransition(old, entry, current.Event) {
				return false
			}
		case "context":
			if !contextRetentionTransition(old, entry, current.Event) {
				return false
			}
		default:
			return false
		}
		transitions[key] = true
		normalized.Entries[i].Payload = old.Payload
	}
	if len(transitions) == 0 {
		return false
	}
	normalized.Gaps = nil
	removed := map[string]bool{}
	for _, gap := range current.Gaps {
		key := gap.Kind + ":" + gap.ID
		if gap.Reason == "disposed_under_retention_policy" && transitions[key] {
			if removed[key] {
				return false
			}
			removed[key] = true
			continue
		}
		normalized.Gaps = append(normalized.Gaps, gap)
	}
	if len(removed) != len(transitions) {
		return false
	}
	if len(normalized.Gaps) == 0 && len(prior.Gaps) == 0 {
		normalized.Gaps = prior.Gaps
	}
	return model.Digest(normalized) == model.Digest(prior)
}

func versionRetentionTransition(old, entry model.AuditEntry, event model.Event) bool {
	var from, to model.Version
	if !decodeRetentionPayload(old.Payload, &from) || !decodeRetentionPayload(entry.Payload, &to) {
		return false
	}
	if from.PayloadDisposed || !to.PayloadDisposed || from.ID != entry.ID || from.ID != event.VersionID || from.RecordID != event.RecordID || from.Proposal.Key.Scope != event.Scope || from.Proposer.Tenant != event.Caller.Tenant || event.RecordKind == "memory" || model.FactDigest(from.Proposal) != from.Digest {
		return false
	}
	return model.Digest(model.VersionTombstone(from)) == model.Digest(to)
}

func memoryVersionRetentionTransition(old, entry model.AuditEntry, event model.Event) bool {
	var from, to model.MemoryVersion
	if !decodeRetentionPayload(old.Payload, &from) || !decodeRetentionPayload(entry.Payload, &to) {
		return false
	}
	if from.PayloadDisposed || !to.PayloadDisposed || from.ID != entry.ID || from.ID != event.VersionID || from.RecordID != event.RecordID || from.Proposal.Key.Scope != event.Scope || from.Proposer.Tenant != event.Caller.Tenant || event.RecordKind != "memory" || model.MemoryDigest(from.Proposal) != from.Digest || from.LedgerRevision <= 0 || from.LedgerRevision > event.LedgerRevision {
		return false
	}
	return model.Digest(model.MemoryVersionTombstone(from)) == model.Digest(to)
}

func snapshotRetentionTransition(old, entry model.AuditEntry, event model.Event) bool {
	var from, to model.FactSnapshot
	if !decodeRetentionPayload(old.Payload, &from) || !decodeRetentionPayload(entry.Payload, &to) {
		return false
	}
	if from.PayloadDisposed || !to.PayloadDisposed || from.ID != entry.ID || from.Tenant != event.Caller.Tenant || from.CreationRevision != event.LedgerRevision || model.SnapshotDigest(from) != from.Digest {
		return false
	}
	from.PayloadDisposed = true
	from.Items = append([]model.SnapshotItem(nil), from.Items...)
	for j := range from.Items {
		from.Items[j].Resolution.Value = nil
	}
	return model.Digest(from) == model.Digest(to)
}

func contextRetentionTransition(old, entry model.AuditEntry, event model.Event) bool {
	var from, to model.ContextManifest
	if !decodeRetentionPayload(old.Payload, &from) || !decodeRetentionPayload(entry.Payload, &to) {
		return false
	}
	if from.PayloadDisposed || !to.PayloadDisposed || from.ID != entry.ID || from.Tenant != event.Caller.Tenant || from.CreationRevision != event.LedgerRevision || model.ContextDigest(from) != from.Digest || model.BytesDigest(from.Rendered) != from.RenderedDigest {
		return false
	}
	if from.CanonicalVersion != "context-manifest-v1" && from.CanonicalVersion != "context-manifest-v2" {
		return false
	}
	return model.Digest(model.ContextTombstone(from)) == model.Digest(to)
}

// Unknown fields cannot be silently discarded when comparing content removal.
func decodeRetentionPayload(raw []byte, target any) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return false
	}
	return decoder.Decode(new(any)) == io.EOF
}
