package workers

import (
	"encoding/json"
	"github.com/caxqueiroz/facts_engine/model"
)

// archiveSnapshotRetentionReplay permits only a full snapshot's transition to
// its exact metadata-only tombstone. All other entries, event fields, scopes
// and coverage gaps must still match the previously signed payload.
func archiveSnapshotRetentionReplay(retained []byte, current model.ArchivePayload) bool {
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
		if entry.Kind != "snapshot" || transitions[entry.ID] {
			return false
		}
		var from, to model.FactSnapshot
		if json.Unmarshal(old.Payload, &from) != nil || json.Unmarshal(entry.Payload, &to) != nil {
			return false
		}
		if from.PayloadDisposed || !to.PayloadDisposed || from.ID != entry.ID || from.Tenant != current.Event.Caller.Tenant || from.CreationRevision != current.Event.LedgerRevision || model.SnapshotDigest(from) != from.Digest {
			return false
		}
		from.PayloadDisposed = true
		from.Items = append([]model.SnapshotItem(nil), from.Items...)
		for j := range from.Items {
			from.Items[j].Resolution.Value = nil
		}
		if model.Digest(from) != model.Digest(to) {
			return false
		}
		transitions[entry.ID] = true
		normalized.Entries[i].Payload = old.Payload
	}
	if len(transitions) == 0 {
		return false
	}
	normalized.Gaps = nil
	removed := map[string]bool{}
	for _, gap := range current.Gaps {
		if gap.Kind == "snapshot_content" && gap.Reason == "disposed_under_retention_policy" && transitions[gap.ID] {
			if removed[gap.ID] {
				return false
			}
			removed[gap.ID] = true
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
