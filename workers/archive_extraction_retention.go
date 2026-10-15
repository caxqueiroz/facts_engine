package workers

import "github.com/caxqueiroz/facts_engine/model"

func extractionRetentionTransition(old, entry model.AuditEntry, event model.Event) bool {
	switch entry.Kind {
	case "extraction_job":
		var from, to model.ExtractionJob
		if !decodeRetentionPayload(old.Payload, &from) || !decodeRetentionPayload(entry.Payload, &to) {
			return false
		}
		if from.PayloadDisposed || !to.PayloadDisposed || from.ID != entry.ID || from.ID != event.EntityID || from.Tenant != event.Caller.Tenant || from.Request.Scope != event.Scope || from.CreatedRevision != event.LedgerRevision || model.ExtractionProfileDigest(from.Profile) != from.ProfileDigest {
			return false
		}
		for _, v := range from.DependencyVersions {
			if v.PayloadDisposed || model.FactDigest(v.Proposal) != v.Digest {
				return false
			}
		}
		return model.Digest(model.ExtractionJobTombstone(from)) == model.Digest(to)
	case "extraction_attempt":
		var from, to model.ExtractionAttempt
		if !decodeRetentionPayload(old.Payload, &from) || !decodeRetentionPayload(entry.Payload, &to) {
			return false
		}
		if from.PayloadDisposed || !to.PayloadDisposed || from.ID != entry.ID || from.ID != event.EntityID || from.Tenant != event.Caller.Tenant || from.Scope != event.Scope || from.LedgerRevision != event.LedgerRevision || model.BytesDigest(from.RequestBytes) != from.RequestDigest {
			return false
		}
		return model.Digest(model.ExtractionAttemptTombstone(from)) == model.Digest(to)
	case "extraction_result":
		var from, to model.ExtractionResult
		if !decodeRetentionPayload(old.Payload, &from) || !decodeRetentionPayload(entry.Payload, &to) {
			return false
		}
		if from.PayloadDisposed || !to.PayloadDisposed || from.AttemptID != entry.ID || from.JobID != event.EntityID || from.Tenant != event.Caller.Tenant || from.Scope != event.Scope || from.LedgerRevision != event.LedgerRevision {
			return false
		}
		// A truncated response's digest identifies the complete provider response;
		// its retained prefix cannot reconstruct that digest. Keep this limitation.
		if from.ResponseComplete && model.BytesDigest(from.ResponseBytes) != from.ResponseDigest {
			return false
		}
		return model.Digest(model.ExtractionResultTombstone(from)) == model.Digest(to)
	}
	return false
}
