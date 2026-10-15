package model

import "time"

// DecisionTombstone retains identity, declared-use lineage and integrity digests
// while removing method, transformation, execution-time and outcome metadata.
func DecisionTombstone(r DecisionRecord) DecisionRecord {
	r.PayloadDisposed = true
	r.Receipt.MethodKind = ""
	r.Receipt.MethodID = ""
	r.Receipt.MethodVersion = ""
	r.Receipt.TransformationVersion = ""
	r.Receipt.DecidedAt = time.Time{}
	r.Receipt.Outcome = ""
	return r
}

// ValidDecisionTombstone rejects content smuggled into a marked envelope.
func ValidDecisionTombstone(r DecisionRecord, tenant, id string) bool {
	return r.PayloadDisposed && r.Tenant == tenant && r.ID == id && Digest(r) == Digest(DecisionTombstone(r))
}
